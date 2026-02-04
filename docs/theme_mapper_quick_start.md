# Conceptual Theme Mapper (CTM) Quick Start

The Conceptual Theme Mapper (CTM) uses topic modeling to reveal how themes shift across a manuscript. It chunks the text (by word count or chapter headings), lemmatizes and filters to nouns/adjectives, then applies LDA to surface dominant themes. The outputs let you track concept variety and narrative progression.

## Setup

1. Install dependencies:

   ```bash
   pip install -r requirements.txt
   ```

2. Download the spaCy English model:

   ```bash
   python -m spacy download en_core_web_sm
   ```

## Run the tool

Analyze a manuscript with default settings (5,000-word chunks, 10 topics):

```bash
python scripts/theme_mapper.py path/to/manuscript.txt --num-topics 10
```

Chunk by chapter headings instead of word count:

```bash
python scripts/theme_mapper.py path/to/manuscript.txt --chunk-mode chapter --chapter-pattern "^CHAPTER\\s+\\d+"
```

## Outputs and how to interpret them

- **Dominant Topics (CLI output)**: Each topic lists its top defining words. Look for groups of nouns/adjectives that describe a consistent idea.
- **Coherence Score**: A higher score suggests cleaner, more interpretable topics. If the score is low, try adjusting `--num-topics`, chunk size, or preprocessing.
- **Topic Distribution CSV** (`topic_distribution.csv`): A table showing each chunk’s topic weights. Use it to see which themes dominate specific chapters.
- **Heatmap PNG** (`topic_heatmap.png`): A visual summary of the distribution table. Bright cells indicate strong thematic focus.
- **Interactive Topic Map** (`topic_map.html`): Open this in a browser to explore topic relationships in conceptual space.

## Tips

- If you see too many mixed themes in each chunk, reduce the chunk size.
- If topics are too fragmented, reduce `--num-topics` or increase chunk size.
- For cleaner themes, ensure the manuscript is plain text without heavy formatting noise.
