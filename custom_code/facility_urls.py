import re


def normalize_facility_url(value, default=''):
    """Unwrap a pasted Markdown link before using a facility URL in requests."""
    url = str(value or '').strip()
    markdown_link = re.fullmatch(r'\[[^\]]*\]\(\s*(https?://.*?)\s*\)', url, flags=re.IGNORECASE)
    if markdown_link:
        url = markdown_link.group(1).strip()
    return url or default
