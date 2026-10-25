# Anime Filename Parser

Parse Anime titles into structured data

## Input Format
Filename (with or without extension)

## Output Format
Return JSON with these fields:
- `media_type`: "anime"
- `title`: Clean title (spaces, proper capitalization)
- `season`: Season number (integer)  
- `episode`: Episode number (integer)
- `episode_title`: Episode title if present (null if not found)
- `confidence`: High/Medium/Low based on pattern match quality

## Simple cases
- When you see SXXEYY - like S01E01 - that means, season: 1, episode: 1 

## Special Cases (Anime Season Detection)
These rules OVERRIDE the default season detection:
- "Jujutsu Kaisen - Shimetsu Kaiyuu" or "Jujutsu Kaisen: Shimetsu Kaiyuu" → season: 3
- "Jujutsu Kaisen 2nd Season" → season: 2
- "Sousou no Frieren" or "Frieren" with "2nd Season" or "Season 2" → title: "Sousou no Frieren", season: 2
- "Ao no Miburo" with "2nd Season" or "Season 2" → title: "Ao no Miburo", season: 2
- Any anime with "2nd Season" in the title → season: 2
- Any anime with "3rd Season" in the title → season: 3
