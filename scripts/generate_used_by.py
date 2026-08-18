#!/usr/bin/env python3
import argparse
import json
import os
import unicodedata
import urllib.request
import xml.etree.ElementTree as ET
from datetime import datetime
from html import escape
from pathlib import Path


THEMES = ("light", "dark")
SVG_NS = "http://www.w3.org/2000/svg"
ET.register_namespace("", SVG_NS)

COPY = {
    "heading": "Projects using tsclient-rs",
    "public": "Public dependents",
    "showing": "Showing",
    "stars": "Total stars",
    "forks": "Total forks",
    "top": "Top {count}",
}

SUMMARY_PALETTES = {
    "light": {
        "card": "#ffffff", "border": "#d8dee4", "accent": "#ff8500",
        "title": "#24292f",
        "text": "#57606a",
        "public_bg": "#f5f0ff", "public_border": "#d8c4ff", "public_text": "#6639ba",
        "top_bg": "#edfff4", "top_border": "#9be9b7", "top_text": "#168244",
        "stars_bg": "#fff4e5", "stars_border": "#ffc46b", "stars_text": "#c45d00",
        "forks_bg": "#edf6ff", "forks_border": "#9ecbff", "forks_text": "#0969da",
    },
    "dark": {
        "card": "#161b22", "border": "#30363d", "accent": "#ffb000",
        "title": "#f0f6fc",
        "text": "#9da7b1",
        "public_bg": "#2d2048", "public_border": "#6e40c9", "public_text": "#d2a8ff",
        "top_bg": "#123621", "top_border": "#238636", "top_text": "#56d364",
        "stars_bg": "#3d2b00", "stars_border": "#9e6a03", "stars_text": "#ffb000",
        "forks_bg": "#102c4c", "forks_border": "#1f6feb", "forks_text": "#58a6ff",
    },
}

PALETTES = {
    "light": {
        "card": "#fffefe",
        "border": "#e4e2e2",
        "title": "#2f80ed",
        "text": "#434d58",
        "muted": "#6e7781",
        "divider": "#e4e2e2",
        "star": "#ff8500",
        "fork": "#0969da",
    },
    "dark": {
        "card": "#161b22",
        "border": "#30363d",
        "title": "#58a6ff",
        "text": "#c9d1d9",
        "muted": "#8b949e",
        "divider": "#30363d",
        "star": "#ffb000",
        "fork": "#58a6ff",
    },
}


def format_date(value):
    date = datetime.fromisoformat(value.replace("Z", "+00:00"))
    months = ("Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec")
    return f"{months[date.month - 1]} {date.day}, {date.year}"


def compact_count(value):
    if value < 1000:
        return str(value)
    amount = value / 1000
    return f"{amount:.1f}".rstrip("0").rstrip(".") + "k"


def display_width(text):
    return sum(2 if unicodedata.east_asian_width(char) in "WFA" else 1 for char in text)


def truncate(text, width):
    result = []
    used = 0
    for char in text:
        char_width = 2 if unicodedata.east_asian_width(char) in "WFA" else 1
        if used + char_width > width - 1:
            return "".join(result).rstrip() + "…"
        result.append(char)
        used += char_width
    return "".join(result)


def wrap_text(text, width=58, lines=2):
    text = " ".join((text or "").split())
    if not text:
        return [""]
    output = []
    remaining = text
    while remaining and len(output) < lines:
        if display_width(remaining) <= width:
            output.append(remaining)
            remaining = ""
            break
        current = []
        used = 0
        last_space = -1
        for index, char in enumerate(remaining):
            char_width = 2 if unicodedata.east_asian_width(char) in "WFA" else 1
            if used + char_width > width:
                break
            current.append(char)
            used += char_width
            if char.isspace():
                last_space = index
        cut = last_space + 1 if last_space >= 0 else len(current)
        output.append(remaining[:cut].strip())
        remaining = remaining[cut:].strip()
    if remaining:
        output[-1] = truncate(output[-1] + " " + remaining, width)
    return output


def font_family():
    return "-apple-system,BlinkMacSystemFont,'Segoe UI','Noto Sans CJK SC','Noto Sans',sans-serif"


def render_card(repository, theme):
    if theme not in THEMES:
        raise ValueError("unsupported theme")
    colors = PALETTES[theme]
    full_name = f"{repository['owner']} / {repository['repo']}"
    name = escape(full_name)
    display_name = escape(truncate(full_name, 32))
    description = escape(repository["descriptions"]["en"])
    description_lines = [escape(line) for line in wrap_text(description, width=38)]
    date = escape(format_date(repository["pushed_at"]))
    date_width = sum(10 if unicodedata.east_asian_width(char) in "WFA" else 5.5 for char in date)
    date_icon_x = max(146, round(266 - date_width - 17))
    stars = compact_count(repository["stargazers_count"])
    forks = compact_count(repository["forks_count"])
    desc_nodes = "".join(
        f'<text x="14" y="{50 + index * 17}" class="description">{line}</text>'
        for index, line in enumerate(description_lines)
    )
    return f'''<svg xmlns="http://www.w3.org/2000/svg" width="280" height="136" viewBox="0 0 280 136" role="img" aria-label="{name}">
  <style>
    text {{ font-family:{font_family()}; }}
    .title {{ font-size:15px;font-weight:600;fill:{colors['title']}; }}
    .description {{ font-size:12px;fill:{colors['text']}; }}
    .metric-value {{ font-size:11px;font-weight:600;fill:{colors['text']}; }}
    .updated {{ font-size:10px;fill:{colors['muted']};letter-spacing:.1px; }}
  </style>
  <rect x="4" y="4" width="272" height="128" rx="10" fill="{colors['card']}" stroke="{colors['border']}"/>
  <text x="14" y="29" class="title">{display_name}</text>
  {desc_nodes}
  <line x1="14" y1="91" x2="266" y2="91" stroke="{colors['divider']}"/>
  <g transform="translate(14 105)">
    <path data-icon="star" d="M8 .75l2.16 4.37 4.82.7-3.49 3.4.82 4.8L8 11.75l-4.31 2.27.82-4.8-3.49-3.4 4.82-.7L8 .75z" fill="{colors['star']}"/>
    <text x="22" y="12" class="metric-value">{stars}</text>
  </g>
  <g transform="translate(79 105)">
    <g data-icon="fork" fill="none" stroke="{colors['fork']}" stroke-width="1.7" stroke-linecap="round" stroke-linejoin="round">
      <circle cx="4" cy="3" r="2"/><circle cx="12" cy="3" r="2"/><circle cx="8" cy="13" r="2"/>
      <path d="M4 5v1c0 2.2 1.8 4 4 4v1M12 5v1c0 2.2-1.8 4-4 4"/>
    </g>
    <text x="22" y="12" class="metric-value">{forks}</text>
  </g>
  <g data-icon="activity" transform="translate({date_icon_x} 107)" fill="none" stroke="{colors['muted']}" stroke-width="1.4" stroke-linecap="round" stroke-linejoin="round">
    <circle cx="6" cy="6" r="5"/>
    <path d="M6 3v3l2 1.5"/>
  </g>
  <text x="266" y="117" text-anchor="end" class="updated">{date}</text>
</svg>'''


def render_summary(public_dependents, shown_count, total_stars, total_forks, theme):
    colors = SUMMARY_PALETTES[theme]
    heading = escape(COPY["heading"])
    public_label = escape(COPY["public"])
    showing_label = escape(COPY["showing"])
    stars_label = escape(COPY["stars"])
    forks_label = escape(COPY["forks"])
    public_value = str(public_dependents)
    showing_value = escape(COPY["top"].format(count=shown_count))
    stars_value = str(total_stars)
    forks_value = str(total_forks)
    return f'''<svg xmlns="http://www.w3.org/2000/svg" width="860" height="140" viewBox="0 0 860 140" role="img" aria-label="{heading}">
  <style>text {{ font-family:{font_family()}; }}</style>
  <rect x="4" y="4" width="852" height="132" rx="8" fill="{colors['card']}" stroke="{colors['border']}"/>
  <rect x="22" y="24" width="5" height="92" rx="2.5" fill="{colors['accent']}"/>
  <text x="46" y="53" font-size="27" font-weight="700" fill="{colors['title']}">{heading}</text>
  <g transform="translate(594 24)"><rect width="116" height="42" rx="7" fill="{colors['public_bg']}" stroke="{colors['public_border']}"/><text x="10" y="17" font-size="9.5" fill="{colors['public_text']}">{public_label}</text><text x="10" y="35" font-size="16" font-weight="700" fill="{colors['public_text']}">{public_value}</text></g>
  <g transform="translate(720 24)"><rect width="116" height="42" rx="7" fill="{colors['top_bg']}" stroke="{colors['top_border']}"/><text x="10" y="17" font-size="9.5" fill="{colors['top_text']}">{showing_label}</text><text x="10" y="35" font-size="16" font-weight="700" fill="{colors['top_text']}">{showing_value}</text></g>
  <g transform="translate(594 76)"><rect width="116" height="42" rx="7" fill="{colors['stars_bg']}" stroke="{colors['stars_border']}"/><text x="10" y="17" font-size="9.5" fill="{colors['stars_text']}">{stars_label}</text><text x="10" y="35" font-size="16" font-weight="700" fill="{colors['stars_text']}">{stars_value}</text></g>
  <g transform="translate(720 76)"><rect width="116" height="42" rx="7" fill="{colors['forks_bg']}" stroke="{colors['forks_border']}"/><text x="10" y="17" font-size="9.5" fill="{colors['forks_text']}">{forks_label}</text><text x="10" y="35" font-size="16" font-weight="700" fill="{colors['forks_text']}">{forks_value}</text></g>
</svg>'''


def render_showcase(repositories, public_dependents, theme):
    # 自适应布局:卡片按 3 列排布,总高度随仓库数增长
    total_stars = sum(repository["stargazers_count"] for repository in repositories)
    total_forks = sum(repository["forks_count"] for repository in repositories)
    rows = (len(repositories) + 2) // 3
    height = 152 + rows * 144
    root = ET.Element(
        f"{{{SVG_NS}}}svg",
        {
            "width": "860",
            "height": str(height),
            "viewBox": f"0 0 860 {height}",
            "role": "img",
            "aria-label": COPY["heading"],
        },
    )
    summary = ET.fromstring(
        render_summary(public_dependents, len(repositories), total_stars, total_forks, theme)
    )
    summary.set("x", "0")
    summary.set("y", "0")
    root.append(summary)
    for index, repository in enumerate(repositories):
        card = ET.fromstring(render_card(repository, theme))
        card.set("x", str((index % 3) * 290))
        card.set("y", str(152 + (index // 3) * 144))
        root.append(card)
    return ET.tostring(root, encoding="unicode")


def generate_assets(repositories, public_dependents, output_dir):
    output_dir.mkdir(parents=True, exist_ok=True)
    repositories = sorted(repositories, key=lambda item: item["stargazers_count"], reverse=True)
    total_stars = sum(repository["stargazers_count"] for repository in repositories)
    total_forks = sum(repository["forks_count"] for repository in repositories)
    expected_files = []
    card_dir = output_dir / "cards"
    card_dir.mkdir(parents=True, exist_ok=True)
    for repository in repositories:
        for theme in THEMES:
            relative = Path("cards") / f"{repository['slug']}-{theme}.svg"
            (output_dir / relative).write_text(render_card(repository, theme), encoding="utf-8")
            expected_files.append(relative.as_posix())
    for theme in THEMES:
        for kind in ("summary", "showcase"):
            relative = Path(f"{kind}-{theme}.svg")
            content = (
                render_summary(public_dependents, len(repositories), total_stars, total_forks, theme)
                if kind == "summary"
                else render_showcase(repositories, public_dependents, theme)
            )
            (output_dir / relative).write_text(content, encoding="utf-8")
            expected_files.append(relative.as_posix())
    manifest = {
        "themes": list(THEMES),
        "repository_count": len(repositories),
        "public_dependents": public_dependents,
        "total_stars": total_stars,
        "total_forks": total_forks,
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "svg_count": len(expected_files),
        "files": sorted(expected_files),
        "repositories": [
            {
                "owner": item["owner"],
                "repo": item["repo"],
                "slug": item["slug"],
                "stars": item["stargazers_count"],
                "forks": item["forks_count"],
                "pushed_at": item["pushed_at"],
            }
            for item in repositories
        ],
    }
    (output_dir / "manifest.json").write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )


def fetch_repository(owner, repo, token=None):
    request = urllib.request.Request(
        f"https://api.github.com/repos/{owner}/{repo}",
        headers={"Accept": "application/vnd.github+json", "User-Agent": "tsclient-rs-used-by-generator"},
    )
    if token:
        request.add_header("Authorization", f"Bearer {token}")
    with urllib.request.urlopen(request, timeout=30) as response:
        return json.load(response)


def build_repositories(config, token=None):
    output = []
    for item in config["repositories"]:
        api = fetch_repository(item["owner"], item["repo"], token)
        description = api.get("description") or f"{item['owner']}/{item['repo']}"
        output.append({
            **item,
            "descriptions": {"en": description},
            "stargazers_count": api["stargazers_count"],
            "forks_count": api["forks_count"],
            "pushed_at": api["pushed_at"],
            "html_url": api["html_url"],
        })
    return output


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", default=".github/used-by-repositories.json")
    parser.add_argument("--output", default="docs/used_by")
    args = parser.parse_args()
    config = json.loads(Path(args.config).read_text(encoding="utf-8"))
    repositories = build_repositories(config, os.environ.get("GITHUB_TOKEN"))
    generate_assets(repositories, config["public_dependents"], Path(args.output))
    print(f"generated {len(repositories)} repository cards into {args.output}")


if __name__ == "__main__":
    main()
