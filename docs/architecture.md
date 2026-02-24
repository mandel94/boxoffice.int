# Architettura Data Product (Data Mesh)

## 1. Product thinking

Il dataset analytics è trattato come prodotto:

- owner espliciti per dominio
- schema contrattualizzato
- output versionabile e discoverable

## 2. Domini e ownership

- **Domain `box_office_raw`**
  - responsabilità: ingestione, parsing e quality check base
  - output: tabella daily per film
- **Domain `film_metadata`**
  - responsabilità: arricchimento metadati esterni
  - output: anagrafica film normalizzata
- **Domain `market_analytics`**
  - responsabilità: KPI, aggregazioni, dataset consumabile BI
  - output: fact giornaliero e KPI giornalieri

## 3. Layer dati

- `data/raw/`: dati estratti dalla fonte senza trasformazioni complesse
- `data/curated/`: normalizzazioni e arricchimenti
- `data/products/`: dataset finali per consumo analitico

## 4. Contratti dati

I contratti sono in `contracts/` e definiscono:

- nomi colonna
- tipi attesi
- nullability minima
- semantica dei campi principali

## 5. Lineage pipeline

1. Scraper Cineguru -> `box_office_raw`
2. Enrichment TMDB -> `film_metadata`
3. Join + KPI -> `market_analytics`

## 6. Quality gate minimi

- colonne obbligatorie presenti
- `rank` tra 1 e 10
- `gross_eur` non negativo
- deduplicazione su (`date`, `rank`, `title`)

## 7. Roadmap industrializzazione

- scheduler giornaliero
- retry policy e alerting
- osservabilità (log strutturati, metriche)
- test qualità dati automatici
- publication layer su DWH
