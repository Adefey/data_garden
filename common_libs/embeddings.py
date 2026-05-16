import logging
import os

import torch
import torch.nn.functional as F
from transformers import AutoModel, AutoTokenizer

logger = logging.getLogger(__name__)

checkpoint = os.environ.get("EMBEDDINGS_MODEL")
prefix_for_feature_extraction = os.environ.get("PREFIX_FOR_FEATURE_EXTRACTION")


class EmbeddingModel:
    def __init__(self):
        self.tokenizer = AutoTokenizer.from_pretrained(checkpoint)
        self.model = AutoModel.from_pretrained(checkpoint)

    def mean_pooling(self, model_output, attention_mask):
        token_embeddings = model_output[0]
        input_mask_expanded = attention_mask.unsqueeze(-1).expand(token_embeddings.size()).float()
        return torch.sum(token_embeddings * input_mask_expanded, 1) / torch.clamp(
            input_mask_expanded.sum(1), min=1e-9
        )

    def get_embeds(self, inputs: str | list[str]) -> list[float] | list[list[float]]:
        if isinstance(inputs, str):
            texts = [prefix_for_feature_extraction + inputs]
        elif isinstance(inputs, list):
            texts = [prefix_for_feature_extraction + t for t in inputs]

        if texts == []:
            return []

        encoded_input = self.tokenizer(texts, padding=True, truncation=True, return_tensors="pt")

        with torch.no_grad():
            model_output = self.model(**encoded_input)

        sentence_embeddings = self.mean_pooling(model_output, encoded_input["attention_mask"])

        sentence_embeddings = F.normalize(sentence_embeddings, p=2, dim=1)

        if isinstance(inputs, str):
            return sentence_embeddings[0].tolist()
        elif isinstance(inputs, list):
            return [emb.tolist() for emb in sentence_embeddings]
        else:
            return None
