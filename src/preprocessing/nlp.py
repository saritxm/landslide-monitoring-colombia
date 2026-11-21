"""NLP Text Preprocessing Module for Landslide Descriptive Fields.

Provides text cleaning, accent normalization, punctuation removal, stopword removal
(English & Spanish), and tokenization for unstructured landslide narratives.
"""

import re
import unicodedata
from typing import List, Optional

# Standard domain stopwords for both English and Spanish relevant to hazard reporting
STOPWORDS_ES = {
    "de", "la", "que", "el", "en", "y", "a", "los", "del", "se", "las", "por", "un", "para",
    "con", "no", "una", "su", "al", "lo", "como", "mas", "pero", "sus", "le", "ya", "o", "este",
    "si", "porque", "esta", "entre", "cuando", "muy", "sin", "sobre", "tambien", "me", "hasta",
    "hay", "donde", "quien", "desde", "todo", "nos", "durante", "todos", "uno", "les", "ni",
    "contra", "otros", "ese", "eso", "ante", "ellos", "e", "esto", "mi", "antes", "algunos",
    "unos", "yo", "otro", "otras", "otra", "el", "tanto", "esa", "estos", "mucho", "quienes",
    "nada", "muchos", "cual", "poco", "ella", "estar", "estas", "algunas", "algo", "nosotros",
    "municipio", "sector", "via", "vereda", "zona", "barrio"
}

STOPWORDS_EN = {
    "i", "me", "my", "myself", "we", "our", "ours", "ourselves", "you", "your", "yours",
    "yourself", "yourselves", "he", "him", "his", "himself", "she", "her", "hers",
    "herself", "it", "its", "itself", "they", "them", "their", "theirs", "themselves",
    "what", "which", "who", "whom", "this", "that", "these", "those", "am", "is", "are",
    "was", "were", "be", "been", "being", "have", "has", "had", "having", "do", "does",
    "did", "doing", "a", "an", "the", "and", "but", "if", "or", "because", "as", "until",
    "while", "of", "at", "by", "for", "with", "about", "against", "between", "into",
    "through", "during", "before", "after", "above", "below", "to", "from", "up", "down",
    "in", "out", "on", "off", "over", "under", "again", "further", "then", "once", "here",
    "there", "when", "where", "why", "how", "all", "any", "both", "each", "few", "more",
    "most", "other", "some", "such", "no", "nor", "not", "only", "own", "same", "so",
    "than", "too", "very", "s", "t", "can", "will", "just", "don", "should", "now"
}

# Workshop #3 refinement: news-reporting words and numbers that dominated the GLC token
# counts in Workshop #2 without describing the event (e.g. "said" was the 3rd most frequent token)
DOMAIN_NOISE = {
    "said", "according", "reported", "near", "km", "kilometers", "miles", "one", "two", "three",
    "four", "five", "least", "also", "around", "located", "occurred", "took", "place",
}

ALL_STOPWORDS = STOPWORDS_ES.union(STOPWORDS_EN).union(DOMAIN_NOISE)


def strip_accents(text: str) -> str:
    """Normalize and remove diacritics/accents from characters."""
    return "".join(
        c for c in unicodedata.normalize("NFD", text)
        if unicodedata.category(c) != "Mn"
    )


def clean_text_for_nlp(text: Optional[str]) -> str:
    """Cleans raw narrative text for NLP analysis.

    Steps:
      1. Handle null / None.
      2. Lowercasing.
      3. Strip accents and diacritics.
      4. Remove punctuation, symbols, and isolated digits.
      5. Collapse consecutive whitespaces.
    """
    if text is None or not isinstance(text, str) or text.strip() == "":
        return ""

    text = text.lower()
    text = strip_accents(text)
    # Remove URL links
    text = re.sub(r"https?://\S+|www\.\S+", "", text)
    # Remove HTML tags if present
    text = re.sub(r"<.*?>", "", text)
    # Keep only alphabetical characters and spaces
    text = re.sub(r"[^a-z\s]", " ", text)
    # Collapse multiple whitespace
    text = re.sub(r"\s+", " ", text).strip()
    return text


def tokenize_and_remove_stopwords(text: Optional[str]) -> List[str]:
    """Cleans text and returns filtered tokens excluding stopwords and short tokens."""
    cleaned = clean_text_for_nlp(text)
    if not cleaned:
        return []
    tokens = [
        word for word in cleaned.split()
        if word not in ALL_STOPWORDS and len(word) > 2
    ]
    return tokens
