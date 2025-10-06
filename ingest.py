import os
import feedparser
import requests
from sumy.parsers.plaintext import PlaintextParser
from sumy.nlp.tokenizers import Tokenizer
from sumy.summarizers.text_rank import TextRankSummarizer
from datetime import datetime, timedelta

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
HEADERS = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type": "application/json"
}

RSS_SOURCES = [
    {"url": "https://www.jeuneafrique.com/feed/", "lang": "fr", "trust": 0.9},
    {"url": "https://african.business/feed/", "lang": "en", "trust": 0.85},
]

def summarize_text(text, lang="en"):
    try:
        parser = PlaintextParser.from_string(text[:2000], Tokenizer(lang))
        summarizer = TextRankSummarizer()
        summary = summarizer(parser.document, 2)
        return " ".join([str(s) for s in summary])
    except:
        return text[:200]

def send_to_supabase(table, data):
    requests.post(f"{SUPABASE_URL}/rest/v1/{table}", headers=HEADERS, json=data)

def main():
    now = datetime.now()
    articles = []
    for source in RSS_SOURCES:
        try:
            feed = feedparser.parse(source["url"])
            for entry in feed.entries[:3]:
                pub_date = datetime(*entry.published_parsed[:6]) if hasattr(entry, 'published_parsed') else now
                if now - pub_date > timedelta(hours=6): continue
                articles.append({
                    "title": entry.title[:200],
                    "summary": summarize_text(entry.summary if hasattr(entry, 'summary') else entry.title, source["lang"]),
                    "lang": source["lang"],
                    "trend_score": 0.75  # score fixe pour MVP
                })
        except Exception as e:
            print(f"RSS error: {e}")

    # Envoyer les articles
    for art in articles[:5]:
        send_to_supabase("atrends", {
            "lang": art["lang"],
            "title": art["title"],
            "summary": art["summary"],
            "trend_score": art["trend_score"],
            "created_at": now.isoformat()
        })
        if art["trend_score"] > 0.7:
            send_to_supabase("breaking", {
                "lang": art["lang"],
                "severity": "🚨 Breaking",
                "message": art["title"][:150],
                "created_at": now.isoformat()
            })

    print(f"✅ {len(articles)} articles traités")

if __name__ == "__main__":
    main()
