use std::sync::Arc;

use async_trait::async_trait;
#[cfg(feature = "speech-runtime")]
use tracing::warn;

use crate::ResolvedAppConfig;

#[derive(Clone, Debug)]
pub struct TranscriptionRecord {
    pub transcript: String,
    pub provider: String,
    pub confidence: Option<f32>,
}

#[async_trait]
pub trait SpeechToTextBackend: Send + Sync {
    async fn transcribe(
        &self,
        bytes: &[u8],
        ext: &str,
        mime_hint: Option<&str>,
    ) -> Option<TranscriptionRecord>;
}

#[derive(Clone)]
#[cfg(not(feature = "speech-runtime"))]
struct DisabledSttBackend;

#[cfg(not(feature = "speech-runtime"))]
#[async_trait]
impl SpeechToTextBackend for DisabledSttBackend {
    async fn transcribe(
        &self,
        _bytes: &[u8],
        _ext: &str,
        _mime_hint: Option<&str>,
    ) -> Option<TranscriptionRecord> {
        None
    }
}

#[cfg(feature = "speech-runtime")]
#[derive(Clone)]
struct LocalWhisperSttBackend {
    whisper_model: Option<String>,
    whisper_bin: String,
}

#[cfg(feature = "speech-runtime")]
#[async_trait]
impl SpeechToTextBackend for LocalWhisperSttBackend {
    async fn transcribe(
        &self,
        bytes: &[u8],
        ext: &str,
        _mime_hint: Option<&str>,
    ) -> Option<TranscriptionRecord> {
        let whisper_model = self.whisper_model.clone()?;
        let whisper_bin = self.whisper_bin.clone();
        let base = std::env::temp_dir().join(format!("aria-whisper-{}", uuid::Uuid::new_v4()));
        let input_path = base.with_extension(ext.trim_start_matches('.'));
        let output_base = base.clone();

        let input_bytes = bytes.to_vec();
        if tokio::task::spawn_blocking({
            let input_path = input_path.clone();
            move || std::fs::write(&input_path, input_bytes)
        })
        .await
        .ok()?
        .is_err()
        {
            return None;
        }

        let status = tokio::process::Command::new(&whisper_bin)
            .arg("-m")
            .arg(&whisper_model)
            .arg("-f")
            .arg(&input_path)
            .arg("-of")
            .arg(&output_base)
            .arg("-otxt")
            .arg("-nt")
            .status()
            .await
            .ok()?;

        if !status.success() {
            let _ = tokio::task::spawn_blocking({
                let input_path = input_path.clone();
                move || std::fs::remove_file(&input_path)
            })
            .await;
            return None;
        }

        let txt_path = output_base.with_extension("txt");
        let transcript = tokio::task::spawn_blocking({
            let txt_path = txt_path.clone();
            move || std::fs::read_to_string(&txt_path)
        })
        .await
        .ok()?
        .ok()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())?;

        let _ = tokio::task::spawn_blocking({
            let input_path = input_path.clone();
            let txt_path = txt_path.clone();
            move || {
                let _ = std::fs::remove_file(&input_path);
                let _ = std::fs::remove_file(&txt_path);
            }
        })
        .await;
        Some(TranscriptionRecord {
            transcript,
            provider: "local_whisper_cpp".to_string(),
            confidence: None,
        })
    }
}

#[cfg(feature = "speech-runtime")]
#[derive(Clone)]
struct CloudHttpSttBackend {
    endpoint: String,
    api_key: Option<String>,
    client: reqwest::Client,
}

#[cfg(feature = "speech-runtime")]
#[async_trait]
impl SpeechToTextBackend for CloudHttpSttBackend {
    async fn transcribe(
        &self,
        bytes: &[u8],
        ext: &str,
        mime_hint: Option<&str>,
    ) -> Option<TranscriptionRecord> {
        use base64::Engine;
        let audio_base64 = base64::engine::general_purpose::STANDARD.encode(bytes);
        let mime_type = mime_hint.map(str::to_string).unwrap_or_else(|| match ext {
            "ogg" => "audio/ogg".to_string(),
            "mp4" => "video/mp4".to_string(),
            _ => "application/octet-stream".to_string(),
        });

        let payload = serde_json::json!({
            "audio_base64": audio_base64,
            "mime_type": mime_type,
            "ext": ext,
        });

        let mut request = self.client.post(&self.endpoint).json(&payload);
        if let Some(api_key) = self.api_key.as_deref() {
            request = request.bearer_auth(api_key.to_string());
        }

        let response = request.send().await.ok()?;
        if !response.status().is_success() {
            warn!(
                endpoint = %self.endpoint,
                status = %response.status(),
                "Cloud STT endpoint returned non-success status"
            );
            return None;
        }
        let json: serde_json::Value = response.json().await.ok()?;
        let transcript = json
            .get("transcript")
            .and_then(|v| v.as_str())
            .map(str::trim)
            .filter(|s| !s.is_empty())?
            .to_string();
        let confidence = json
            .get("confidence")
            .and_then(|v| v.as_f64())
            .map(|v| v as f32);
        let provider = json
            .get("provider")
            .and_then(|v| v.as_str())
            .map(str::to_string)
            .unwrap_or_else(|| "cloud_http".to_string());
        Some(TranscriptionRecord {
            transcript,
            provider,
            confidence,
        })
    }
}

#[cfg(feature = "speech-runtime")]
#[derive(Clone)]
struct CompositeSttBackend {
    primary: Arc<dyn SpeechToTextBackend>,
    fallback: Option<Arc<dyn SpeechToTextBackend>>,
}

#[cfg(feature = "speech-runtime")]
#[async_trait]
impl SpeechToTextBackend for CompositeSttBackend {
    async fn transcribe(
        &self,
        bytes: &[u8],
        ext: &str,
        mime_hint: Option<&str>,
    ) -> Option<TranscriptionRecord> {
        if let Some(result) = self.primary.transcribe(bytes, ext, mime_hint).await {
            return Some(result);
        }
        let Some(fallback) = &self.fallback else {
            return None;
        };
        fallback.transcribe(bytes, ext, mime_hint).await
    }
}

pub fn build_stt_backend(
    config: &ResolvedAppConfig,
    client: reqwest::Client,
) -> Arc<dyn SpeechToTextBackend> {
    #[cfg(not(feature = "speech-runtime"))]
    {
        let _ = config;
        let _ = client;
        return Arc::new(DisabledSttBackend);
    }

    #[cfg(feature = "speech-runtime")]
    {
        let cloud_backend = config
            .gateway
            .stt_cloud_endpoint
            .as_deref()
            .map(str::trim)
            .filter(|v| !v.is_empty())
            .map(|endpoint| {
                Arc::new(CloudHttpSttBackend {
                    endpoint: endpoint.to_string(),
                    api_key: crate::non_empty_env(&config.gateway.stt_cloud_api_key_env),
                    client: client.clone(),
                }) as Arc<dyn SpeechToTextBackend>
            });

        let primary: Arc<dyn SpeechToTextBackend> = match config
            .gateway
            .stt_backend
            .trim()
            .to_ascii_lowercase()
            .as_str()
        {
            "cloud_http" => cloud_backend.clone().unwrap_or_else(|| {
                Arc::new(LocalWhisperSttBackend {
                    whisper_model: config.runtime.whisper_cpp_model.clone(),
                    whisper_bin: config.runtime.whisper_cpp_bin.clone(),
                })
            }),
            _ => Arc::new(LocalWhisperSttBackend {
                whisper_model: config.runtime.whisper_cpp_model.clone(),
                whisper_bin: config.runtime.whisper_cpp_bin.clone(),
            }),
        };
        let fallback = if config.gateway.stt_cloud_fallback {
            cloud_backend
        } else {
            None
        };
        Arc::new(CompositeSttBackend { primary, fallback })
    }
}
