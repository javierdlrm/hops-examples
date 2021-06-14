class Transformer(object):
    def __init__(self):
        print("[Transformer] Initializing...")
        # Initialization code goes here

    def preprocess(self, inputs):
        print("[Transformer] Preprocessing...")
        # Transform the requests inputs here. The object returned by this method will be used as model input.
        return inputs

    def postprocess(self, outputs):
        print("[Transformer] Postprocessing...")
        # Transform the predictions computed by the model before returning a response.
        return outputs