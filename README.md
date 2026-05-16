# Data Garden

## Microservices

### 1. Data streamer
- Gathers news from RSS channels
- Provides 2 websockets: for instant news without any clustering and same news but with cluster_id

### 2. Embeddings
- Hosts embedding model for feature extraction from news
- Uses DBSTREAM for news clusterization