use scraper::{ElementRef, Html, Selector};

fn decode_common_html_entities(text: &str) -> String {
    let mut decoded = text
        .replace("&nbsp;", " ")
        .replace("&amp;", "&")
        .replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&quot;", "\"")
        .replace("&#39;", "'")
        .replace("&apos;", "'");
    while let Some(start) = decoded.find("&#") {
        let Some(end_rel) = decoded[start..].find(';') else {
            break;
        };
        let end = start + end_rel;
        let entity = &decoded[start + 2..end];
        let parsed = if let Some(hex) = entity
            .strip_prefix('x')
            .or_else(|| entity.strip_prefix('X'))
        {
            u32::from_str_radix(hex, 16).ok()
        } else {
            entity.parse::<u32>().ok()
        };
        let Some(value) = parsed.and_then(char::from_u32) else {
            break;
        };
        decoded.replace_range(start..=end, &value.to_string());
    }
    decoded
}

fn normalize_extracted_text(text: &str) -> String {
    let mut normalized = String::with_capacity(text.len());
    let mut previous_was_space = false;
    for ch in text.chars() {
        let mapped = match ch {
            '\u{00a0}' => ' ',
            _ => ch,
        };
        if mapped.is_whitespace() {
            if !previous_was_space {
                normalized.push(' ');
                previous_was_space = true;
            }
        } else {
            normalized.push(mapped);
            previous_was_space = false;
        }
    }
    normalized.trim().to_string()
}

fn truncate_text_boundary(text: &str, limit: usize) -> String {
    if text.chars().count() <= limit {
        return text.to_string();
    }
    let mut out = String::new();
    for ch in text.chars().take(limit) {
        out.push(ch);
    }
    out.trim_end().to_string()
}

fn lower_contains_any(text: &str, needles: &[&str]) -> bool {
    needles.iter().any(|needle| text.contains(needle))
}

fn strip_html_to_text_with_structure(html: &str) -> String {
    let mut out = String::with_capacity(html.len());
    let mut in_tag = false;
    let mut tag_buf = String::new();
    for ch in html.chars() {
        match ch {
            '<' if !in_tag => {
                in_tag = true;
                tag_buf.clear();
            }
            '>' if in_tag => {
                let tag = tag_buf.trim().to_ascii_lowercase();
                let is_closing = tag.starts_with('/');
                let tag_name = tag
                    .trim_start_matches('/')
                    .split_whitespace()
                    .next()
                    .unwrap_or("");
                let block_like = matches!(
                    tag_name,
                    "p"
                        | "div"
                        | "section"
                        | "article"
                        | "main"
                        | "aside"
                        | "header"
                        | "footer"
                        | "nav"
                        | "ul"
                        | "ol"
                        | "li"
                        | "table"
                        | "tr"
                        | "td"
                        | "th"
                        | "blockquote"
                        | "pre"
                        | "br"
                        | "h1"
                        | "h2"
                        | "h3"
                        | "h4"
                        | "h5"
                        | "h6"
                );
                if tag_name == "li" && !is_closing {
                    out.push_str(" - ");
                } else if block_like {
                    out.push('\n');
                }
                in_tag = false;
            }
            _ if in_tag => tag_buf.push(ch),
            _ => out.push(ch),
        }
    }
    out
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ExtractionProfile {
    Generic,
    Docs,
    Blog,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SiteExtractionAdapter {
    DocsRs,
    GitHubDocs,
    GitHubRepository,
    Mdn,
    GenericBlog,
    ReadTheDocs,
}

impl SiteExtractionAdapter {
    fn as_str(self) -> &'static str {
        match self {
            SiteExtractionAdapter::DocsRs => "docs_rs",
            SiteExtractionAdapter::GitHubDocs => "github_docs",
            SiteExtractionAdapter::GitHubRepository => "github_repository",
            SiteExtractionAdapter::Mdn => "mdn",
            SiteExtractionAdapter::GenericBlog => "generic_blog",
            SiteExtractionAdapter::ReadTheDocs => "read_the_docs",
        }
    }
}

impl ExtractionProfile {
    fn as_str(self) -> &'static str {
        match self {
            ExtractionProfile::Generic => "generic",
            ExtractionProfile::Docs => "docs",
            ExtractionProfile::Blog => "blog",
        }
    }
}

fn detect_extraction_profile(url: Option<&str>) -> ExtractionProfile {
    let Some(url) = url else {
        return ExtractionProfile::Generic;
    };
    let Ok(parsed) = reqwest::Url::parse(url) else {
        return ExtractionProfile::Generic;
    };
    let host = parsed.host_str().unwrap_or_default().to_ascii_lowercase();
    let path = parsed.path().to_ascii_lowercase();
    let combined = format!("{}{}", host, path);
    if lower_contains_any(
        &combined,
        &[
            "docs.",
            "/docs",
            "/doc",
            "/guide",
            "/guides",
            "/manual",
            "/reference",
            "/api",
            "/apidocs",
        ],
    ) {
        return ExtractionProfile::Docs;
    }
    if lower_contains_any(
        &combined,
        &[
            "blog.",
            "/blog",
            "/blogs",
            "/post",
            "/posts",
            "/news",
            "/article",
            "/articles",
        ],
    ) {
        return ExtractionProfile::Blog;
    }
    ExtractionProfile::Generic
}

fn detect_site_adapter(url: Option<&str>) -> Option<SiteExtractionAdapter> {
    let Some(url) = url else {
        return None;
    };
    let Ok(parsed) = reqwest::Url::parse(url) else {
        return None;
    };
    let host = parsed.host_str().unwrap_or_default().to_ascii_lowercase();
    let path = parsed.path().to_ascii_lowercase();
    if host == "docs.rs" {
        return Some(SiteExtractionAdapter::DocsRs);
    }
    if host == "docs.github.com" {
        return Some(SiteExtractionAdapter::GitHubDocs);
    }
    if host == "github.com" {
        let segments = path
            .split('/')
            .filter(|segment| !segment.is_empty())
            .collect::<Vec<_>>();
        if segments.len() >= 2 {
            return Some(SiteExtractionAdapter::GitHubRepository);
        }
    }
    if host == "developer.mozilla.org" || host == "developer.mozilla.org." {
        return Some(SiteExtractionAdapter::Mdn);
    }
    if host.ends_with(".readthedocs.io") || host == "readthedocs.io" {
        return Some(SiteExtractionAdapter::ReadTheDocs);
    }
    if host.starts_with("blog.") || path.contains("/blog/") || path.contains("/posts/") {
        return Some(SiteExtractionAdapter::GenericBlog);
    }
    None
}

fn extraction_profile_noise_hints(profile: ExtractionProfile) -> &'static [&'static str] {
    match profile {
        ExtractionProfile::Generic => &[
            "nav", "menu", "footer", "header", "sidebar", "aside", "comment", "comments",
            "breadcrumb", "share", "related", "promo", "advert", "ads", "cookie", "modal",
            "toolbar", "pagination",
        ],
        ExtractionProfile::Docs => &[
            "nav", "menu", "footer", "header", "sidebar", "aside", "comment", "comments",
            "breadcrumb", "share", "related", "promo", "advert", "ads", "cookie", "modal",
            "toolbar", "pagination", "toc", "table-of-contents", "on-this-page", "edit-page",
            "edit-this-page", "docs-nav",
        ],
        ExtractionProfile::Blog => &[
            "nav", "menu", "footer", "header", "sidebar", "aside", "comment", "comments",
            "breadcrumb", "share", "related", "promo", "advert", "ads", "cookie", "modal",
            "toolbar", "pagination", "newsletter", "subscribe", "author-card", "social",
        ],
    }
}

fn site_adapter_noise_hints(adapter: Option<SiteExtractionAdapter>) -> &'static [&'static str] {
    match adapter {
        Some(SiteExtractionAdapter::DocsRs) => &[
            "sidebar-elems",
            "sub-location",
            "out-of-band",
            "rustdoc-toolbar",
            "search-container",
        ],
        Some(SiteExtractionAdapter::GitHubDocs) => &[
            "table-of-contents",
            "toc",
            "breadcrumbs",
            "article-footer",
            "sidebar",
        ],
        Some(SiteExtractionAdapter::GitHubRepository) => &[
            "repository-sidebar",
            "discussion-sidebar",
            "js-repo-nav",
            "underline-nav",
            "footer",
            "breadcrumb",
            "toc",
        ],
        Some(SiteExtractionAdapter::Mdn) => &[
            "left-sidebar",
            "sidebar-quicklinks",
            "page-sidebar",
            "on-this-page",
            "breadcrumbs",
        ],
        Some(SiteExtractionAdapter::GenericBlog) => &[
            "newsletter",
            "subscribe",
            "author-card",
            "share",
            "related-posts",
            "comments",
        ],
        Some(SiteExtractionAdapter::ReadTheDocs) => &[
            "wy-nav-side",
            "wy-side-nav-search",
            "wy-menu",
            "rst-footer-buttons",
            "breadcrumbs",
            "toc",
        ],
        None => &[],
    }
}

fn extraction_profile_positive_hints(profile: ExtractionProfile) -> &'static [&'static str] {
    match profile {
        ExtractionProfile::Generic => &[
            "main", "article", "content", "post", "entry", "markdown", "document", "docs", "prose",
        ],
        ExtractionProfile::Docs => &[
            "main", "article", "content", "markdown", "document", "docs", "prose", "api",
            "reference", "manual",
        ],
        ExtractionProfile::Blog => &[
            "main", "article", "content", "post", "entry", "story", "blog", "prose",
        ],
    }
}

fn site_adapter_positive_hints(adapter: Option<SiteExtractionAdapter>) -> &'static [&'static str] {
    match adapter {
        Some(SiteExtractionAdapter::DocsRs) => &[
            "rustdoc",
            "docblock",
            "main-content",
            "content",
            "item-info",
        ],
        Some(SiteExtractionAdapter::GitHubDocs) => &[
            "markdown-body",
            "article",
            "article-body",
            "main-content",
            "content",
        ],
        Some(SiteExtractionAdapter::GitHubRepository) => &[
            "markdown-body",
            "repository-content",
            "wiki-wrapper",
            "blob-wrapper",
            "readme",
        ],
        Some(SiteExtractionAdapter::Mdn) => &[
            "main-page-content",
            "article-body",
            "article",
            "content",
            "section-content",
        ],
        Some(SiteExtractionAdapter::GenericBlog) => &[
            "post-content",
            "post-body",
            "entry-content",
            "article-body",
            "content",
        ],
        Some(SiteExtractionAdapter::ReadTheDocs) => &[
            "wy-nav-content",
            "rst-content",
            "document",
            "section",
            "content",
        ],
        None => &[],
    }
}

fn selector(pattern: &str) -> Selector {
    Selector::parse(pattern).expect("static selector should parse")
}

fn collect_structured_text(element: ElementRef<'_>) -> String {
    normalize_extracted_text(&decode_common_html_entities(&strip_html_to_text_with_structure(
        &element.html(),
    )))
}

fn element_attr_fingerprint(element: ElementRef<'_>) -> String {
    let value = element.value();
    let id = value.id().unwrap_or_default();
    let classes = value.attr("class").unwrap_or_default();
    let role = value.attr("role").unwrap_or_default();
    let aria = value.attr("aria-label").unwrap_or_default();
    let data_testid = value.attr("data-testid").unwrap_or_default();
    format!(
        "{} {} {} {} {} {}",
        value.name(),
        id,
        classes,
        role,
        aria,
        data_testid
    )
    .to_ascii_lowercase()
}

fn html_candidate_score(
    element: ElementRef<'_>,
    profile: ExtractionProfile,
    adapter: Option<SiteExtractionAdapter>,
) -> (i64, String) {
    let attrs = element_attr_fingerprint(element);
    let text = collect_structured_text(element);
    let text_len = text.chars().count() as i64;
    let paragraph_count = element.select(&selector("p")).count() as i64;
    let heading_count = element.select(&selector("h1, h2, h3")).count() as i64;
    let link_count = element.select(&selector("a")).count() as i64;
    let list_item_count = element.select(&selector("li")).count() as i64;

    let mut score = text_len;
    score += paragraph_count * 120;
    score += heading_count * 80;
    score += list_item_count * 15;
    score -= link_count * 30;

    if lower_contains_any(&attrs, extraction_profile_positive_hints(profile)) {
        score += 400;
    }
    if lower_contains_any(&attrs, site_adapter_positive_hints(adapter)) {
        score += 600;
    }
    if lower_contains_any(&attrs, extraction_profile_noise_hints(profile)) {
        score -= 800;
    }
    if lower_contains_any(&attrs, site_adapter_noise_hints(adapter)) {
        score -= 1000;
    }

    (score, text)
}

fn choose_site_adapter_preferred_region<'a>(
    document: &'a Html,
    profile: ExtractionProfile,
    adapter: Option<SiteExtractionAdapter>,
) -> Option<ElementRef<'a>> {
    let hints = site_adapter_positive_hints(adapter);
    if hints.is_empty() {
        return None;
    }
    let candidates_selector = selector("main, article, section, div");
    let candidates = document.select(&candidates_selector);
    let mut best: Option<(i64, ElementRef<'a>)> = None;
    for element in candidates {
        let attrs = element_attr_fingerprint(element);
        if !lower_contains_any(&attrs, hints) {
            continue;
        }
        let (score, _) = html_candidate_score(element, profile, adapter);
        if best
            .as_ref()
            .map(|(best_score, _)| score > *best_score)
            .unwrap_or(true)
        {
            best = Some((score, element));
        }
    }
    best.map(|(_, element)| element)
}

fn readability_candidate_selectors() -> &'static str {
    "main, article, section, div, body"
}

fn choose_best_content_region<'a>(
    document: &'a Html,
    profile: ExtractionProfile,
    adapter: Option<SiteExtractionAdapter>,
) -> Option<ElementRef<'a>> {
    if let Some(exact) = choose_site_adapter_preferred_region(document, profile, adapter) {
        return Some(exact);
    }

    let mut best: Option<(i64, ElementRef<'a>)> = None;
    for element in document.select(&selector(readability_candidate_selectors())) {
        let (score, text) = html_candidate_score(element, profile, adapter);
        if score <= 0 || text.is_empty() {
            continue;
        }
        if best
            .as_ref()
            .map(|(best_score, _)| score > *best_score)
            .unwrap_or(true)
        {
            best = Some((score, element));
        }
    }
    best.map(|(_, element)| element)
}

fn build_excerpt(text: &str) -> Option<String> {
    if text.is_empty() {
        return None;
    }
    let sentence_excerpt = text
        .split_terminator(['.', '!', '?'])
        .map(str::trim)
        .filter(|part| !part.is_empty())
        .take(2)
        .collect::<Vec<_>>()
        .join(". ");
    let candidate = if sentence_excerpt.is_empty() {
        text.to_string()
    } else if sentence_excerpt.ends_with('.') {
        sentence_excerpt
    } else {
        format!("{}.", sentence_excerpt)
    };
    Some(truncate_text_boundary(&candidate, 240))
}

fn document_title(document: &Html) -> Option<String> {
    document
        .select(&selector("title"))
        .next()
        .map(collect_structured_text)
        .filter(|value| !value.is_empty())
}

fn document_headings(element: ElementRef<'_>) -> Vec<String> {
    element
        .select(&selector("h1, h2, h3"))
        .map(collect_structured_text)
        .filter(|value| !value.is_empty())
        .fold(Vec::<String>::new(), |mut acc, heading| {
            if !acc.iter().any(|existing| existing == &heading) && acc.len() < 6 {
                acc.push(heading);
            }
            acc
        })
}

fn extract_html_content_for_url(url: Option<&str>, html: &str) -> HtmlExtractionResult {
    let profile = detect_extraction_profile(url);
    let site_adapter = detect_site_adapter(url);
    let document = Html::parse_document(html);
    let title = document_title(&document);

    let content_element = choose_best_content_region(&document, profile, site_adapter)
        .or_else(|| document.select(&selector("body")).next());

    let (text, headings) = if let Some(element) = content_element {
        (collect_structured_text(element), document_headings(element))
    } else {
        let root_text = normalize_extracted_text(&decode_common_html_entities(
            &strip_html_to_text_with_structure(&document.root_element().html()),
        ));
        (root_text, Vec::new())
    };

    HtmlExtractionResult {
        excerpt: build_excerpt(&text),
        text,
        title,
        headings,
        profile: profile.as_str(),
        site_adapter: site_adapter.map(|adapter| adapter.as_str()),
    }
}

fn extract_html_content(html: &str) -> HtmlExtractionResult {
    extract_html_content_for_url(None, html)
}

fn extract_html_text(html: &str) -> String {
    extract_html_content(html).text
}

fn extract_html_links(base: &reqwest::Url, html: &str) -> Vec<reqwest::Url> {
    let document = Html::parse_document(html);
    let mut links = Vec::new();
    let mut seen = HashSet::new();
    for element in document.select(&selector("a[href]")) {
        let Some(href) = element.value().attr("href") else {
            continue;
        };
        if href.is_empty()
            || href.starts_with('#')
            || href.starts_with("javascript:")
            || href.starts_with("mailto:")
            || href.starts_with("tel:")
        {
            continue;
        }
        let Ok(resolved) = base.join(href) else {
            continue;
        };
        let key = resolved.as_str().to_string();
        if seen.insert(key) {
            links.push(resolved);
        }
    }
    links
}

async fn fetch_web_document(url: &str) -> Result<(String, String), OrchestratorError> {
    let validated = validate_web_url_target_runtime(url, private_network_override_enabled()).await?;
    let domain = url_host_key(&validated)
        .ok()
        .unwrap_or_else(|| "unknown".into());
    let client = reqwest::Client::new();
    let policy = web_rate_policy();
    for attempt in 0..=policy.fetch_retry_attempts {
        throttle_web_domain_request(&domain).await;
        let response = client.get(validated.clone()).send().await.map_err(|e| {
            OrchestratorError::ToolError(format!("Failed to fetch '{}': {}", url, e))
        })?;
        if response.status().is_success() {
            let content_type = response
                .headers()
                .get(reqwest::header::CONTENT_TYPE)
                .and_then(|value| value.to_str().ok())
                .unwrap_or("text/html; charset=utf-8")
                .to_string();
            let body = response.text().await.map_err(|e| {
                OrchestratorError::ToolError(format!("Failed to read '{}': {}", url, e))
            })?;
            return Ok((body, content_type));
        }
        let status = response.status();
        if attempt >= policy.fetch_retry_attempts || !retryable_web_status(status) {
            return Err(OrchestratorError::ToolError(format!(
                "Failed to fetch '{}': status {}",
                url, status
            )));
        }
        let retry_after_ms = parse_retry_after_delay_ms(
            response.headers().get(reqwest::header::RETRY_AFTER),
        )
        .unwrap_or_else(|| {
            let factor = 1_u64 << attempt.min(16);
            (policy.fetch_retry_base_delay_ms.saturating_mul(factor))
                .min(policy.fetch_retry_max_delay_ms)
        });
        tokio::time::sleep(std::time::Duration::from_millis(retry_after_ms)).await;
    }
    Err(OrchestratorError::ToolError(format!(
        "Failed to fetch '{}': retries exhausted",
        url
    )))
}

async fn fetch_web_bytes(url: &str) -> Result<(Vec<u8>, String), OrchestratorError> {
    let validated = validate_web_url_target_runtime(url, private_network_override_enabled()).await?;
    let domain = url_host_key(&validated)
        .ok()
        .unwrap_or_else(|| "unknown".into());
    let client = reqwest::Client::new();
    let policy = web_rate_policy();
    for attempt in 0..=policy.fetch_retry_attempts {
        throttle_web_domain_request(&domain).await;
        let response = client.get(validated.clone()).send().await.map_err(|e| {
            OrchestratorError::ToolError(format!("Failed to fetch '{}': {}", url, e))
        })?;
        if response.status().is_success() {
            let content_type = response
                .headers()
                .get(reqwest::header::CONTENT_TYPE)
                .and_then(|value| value.to_str().ok())
                .unwrap_or("application/octet-stream")
                .to_string();
            let body = response.bytes().await.map_err(|e| {
                OrchestratorError::ToolError(format!("Failed to read '{}': {}", url, e))
            })?;
            return Ok((body.to_vec(), content_type));
        }
        let status = response.status();
        if attempt >= policy.fetch_retry_attempts || !retryable_web_status(status) {
            return Err(OrchestratorError::ToolError(format!(
                "Failed to fetch '{}': status {}",
                url, status
            )));
        }
        let retry_after_ms = parse_retry_after_delay_ms(
            response.headers().get(reqwest::header::RETRY_AFTER),
        )
        .unwrap_or_else(|| {
            let factor = 1_u64 << attempt.min(16);
            (policy.fetch_retry_base_delay_ms.saturating_mul(factor))
                .min(policy.fetch_retry_max_delay_ms)
        });
        tokio::time::sleep(std::time::Duration::from_millis(retry_after_ms)).await;
    }
    Err(OrchestratorError::ToolError(format!(
        "Failed to fetch '{}': retries exhausted",
        url
    )))
}

fn path_for_url(url: &reqwest::Url) -> String {
    let path = url.path().trim();
    if path.is_empty() {
        "/".into()
    } else if path.starts_with('/') {
        path.to_string()
    } else {
        format!("/{}", path)
    }
}
