import os

import torch
import torch.nn.functional as F
from transformers import AutoModel, AutoTokenizer

checkpoint = os.environ.get("EMBEDDINGS_MODEL")


def mean_pooling(model_output, attention_mask):
    token_embeddings = model_output[0]
    input_mask_expanded = attention_mask.unsqueeze(-1).expand(token_embeddings.size()).float()
    return torch.sum(token_embeddings * input_mask_expanded, 1) / torch.clamp(
        input_mask_expanded.sum(1), min=1e-9
    )


tokenizer = AutoTokenizer.from_pretrained(checkpoint)
model = AutoModel.from_pretrained(checkpoint)


def get_embeds(inputs: str | list[str]):
    texts = []

    if isinstance(inputs, str):
        texts.append(inputs)
    elif isinstance(inputs, list):
        texts.extend(inputs)

    encoded_input = tokenizer(texts, padding=True, truncation=True, return_tensors="pt")

    with torch.no_grad():
        model_output = model(**encoded_input)

    sentence_embeddings = mean_pooling(model_output, encoded_input["attention_mask"])

    sentence_embeddings = F.normalize(sentence_embeddings, p=2, dim=1)

    if isinstance(inputs, str):
        return sentence_embeddings[0]
    elif isinstance(inputs, list):
        return sentence_embeddings
    else:
        return None
