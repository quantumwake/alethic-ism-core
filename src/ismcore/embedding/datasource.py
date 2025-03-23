import embedding_utils as util

class BaseDataSource:
    pass

class EmbeddingDataSource(BaseDataSource):
    def create_embedding(text: str):
        util.create_embedding(text=text, model_name='st_minilm_l6_v2')

