"""Simple model for testing."""

def predict(x):
    """Simple prediction function."""
    return x * 2

class Model:
    def __init__(self):
        self.trained = False
    
    def fit(self, data):
        self.trained = True
        return self
    
    def predict(self, data):
        if not self.trained:
            raise ValueError("Model not trained")
        return [predict(x) for x in data]