import os
import feedparser
import requests
from sumy.parsers.plaintext import PlaintextParser
from sumy.nlp.tokenizers import Tokenizer
from sumy.summarizers.text_rank import TextRankSummarizer
from collections import defaultdict, Counter
from datetime import datetime, timedelta
import hashlib
import time

# 🔑 Configuration
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
HEADERS = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type": "application/json",
    "Prefer": "return=minimal"
}

# 🌍 Sources RSS africaines fiables (MVP)
RSS_SOURCES = [
    {"url": "https://www.jeuneafrique.com/feed/", "lang": "fr", "trust": 0.9},
    {"url": "https://african.business/feed/", "lang": "en", "trust": 0.85},
    {"url": "https://www.bloomberg.com/africa/rss", "lang": "en", "trust": 0.95},
    {"url": "https://cnbcafrica.com/feed/", "lang": "en", "trust": 0.8},
    {"url": "https://www.agenceecofin.com/feed", "lang": "fr", "trust": 0.85},
]

# 🌐 Langues cibles
TARGET_LANGS = ["fr", "en", "es", "ar", "pt", "sw"]

def fetch_articles():
    articles = []
    for source in RSS_SOURCES:
        try:
            feed = feedparser.parse(source["url"])
            for entry in feed.entries[:5]:  # 5 derniers articles
                pub_date = datetime(*entry.published_parsed[:6]) if hasattr(entry, 'published_parsed') else datetime.now()
                if datetime.now() - pub_date > timedelta(hours=6):
                    continue
                articles.append({
                    "title": entry.title,
                    "summary": entry.summary if hasattr(entry, 'summary') else entry.title,
                    "link": entry.link,
                    "lang": source["lang"],
                    "source_trust": source["trust"],
                    "pub_date": pub_date.isoformat()
                })
        except Exception as e:
            print(f"Erreur RSS {source['url']}: {e}")
    return articles

def cluster_articles(articles):
    clusters = defaultdict(list)
    for art in articles:
        # Clustering simple : hachage du titre normalisé
        key = hashlib.md5(art["title"].lower().encode()).hexdigest()[:16]
        clusters[key].append(art)
    return list(clusters.values())

def compute_trend_score(cluster, now):
    n = len(cluster)
    if n == 0:
        return 0.0

    # Fréquence (nombre d'articles)
    freq = n

    # Croissance (articles récents = plus de poids)
    recent_count = sum(1 for a in cluster if datetime.fromisoformat(a["pub_date"].replace("Z", "+00:00")) > now - timedelta(hours=1))
    growth = recent_count / n if n > 0 else 0

    # Confiance source (moyenne pondérée)
    source_trust = sum(a["source_trust"] for a in cluster) / n

    # Social & internal engagement = 0 pour MVP (à activer en Phase 2)
    social = 0.0
    internal_engagement = 0.0

    # Normalisation (min-max simple sur les clusters du batch)
    # Pour MVP, on normalise manuellement entre 0 et 1
    norm_freq = min(freq / 5.0, 1.0)
    norm_growth = growth
    norm_social = social
    norm_internal = internal_engagement
    norm_trust = source_trust

    # Formule MVP
    score = (
        0.30 * norm_freq +
        0.25 * norm_growth +
        0.20 * norm_social +
        0.15 * norm_trust +
        0.10 * norm_internal
    )
    return round(score, 2)

def summarize_text(text, lang="en"):
    try:
        parser = PlaintextParser.from_string(text, Tokenizer(lang))
        summarizer = TextRankSummarizer()
        summary = summarizer(parser.document, 3)  # 3 phrases
        return " ".join([str(sentence) for sentence in summary])
    except:
        return text[:300] + "..."

def translate_text(text, target_lang):
    if not text or target_lang == "fr":
        return text
    try:
        from googletrans import Translator
        translator = Translator()
        result = translator.translate(text, src='fr', dest=target_lang)
        return result.text
    except:
        return text  # fallback

def send_to_supabase(table, data):
    try:
        response = requests.post(
            f"{SUPABASE_URL}/rest/v1/{table}",
            headers=HEADERS,
            json=data
        )
        if response.status_code not in [200, 201]:
            print(f"Erreur Supabase {table}: {response.text}")
    except Exception as e:
        print(f"Exception Supabase {table}: {e}")

def main():
    print("🚀 Démarrage ingestion Umoja Insights")
    now = datetime.now()

    # 1. Récupérer les articles
    articles = fetch_articles()
    if not articles:
        print("⚠️ Aucun article trouvé")
        return

    # 2. Clusteriser
    clusters = cluster_articles(articles)

    # 3. Traiter chaque cluster
    for cluster in clusters[:10]:  # max 10 clusters par run
        score = compute_trend_score(cluster, now)
        if score < 0.3:
            continue  # seuil minimal

        main_art = cluster[0]
        base_title = main_art["title"]
        base_summary = summarize_text(main_art["summary"], main_art["lang"])

        # Déterminer la langue de base (priorité FR > EN)
        base_lang = "fr" if any(a["lang"] == "fr" for a in cluster) else "en"

        # Traduire dans toutes les langues
        translations = {}
        for lang in TARGET_LANGS:
            if lang == base_lang:
                translations[lang] = {"title": base_title, "summary": base_summary}
            else:
                translations[lang] = {
                    "title": translate_text(base_title, lang),
                    "summary": translate_text(base_summary, lang)
                }

        # Envoyer chaque traduction
        for lang, content in translations.items():
            data = {
                "lang": lang,
                "title": content["title"][:250],
                "summary": content["summary"][:1000],
                "trend_score": score,
                "created_at": now.isoformat()
            }
            send_to_supabase("atrends", data)

        # Si score > 0.8 → breaking news
        if score > 0.8:
            severity = "🚨 Breaking" if score > 0.9 else "⚠️ Urgent"
            for lang in TARGET_LANGS:
                breaking_data = {
                    "lang": lang,
                    "severity": severity,
                    "message": translations[lang]["title"][:180],
                    "created_at": now.isoformat()
                }
                send_to_supabase("breaking", breaking_data)

    print(f"✅ Ingestion terminée : {len(clusters)} clusters traités")

if __name__ == "__main__":
    main()
