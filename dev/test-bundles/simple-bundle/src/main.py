#!/usr/bin/env python3
"""
Simple test model for local development testing.
"""

def predict(data):
    """Simple prediction function."""
    return {"prediction": f"processed_{data}", "confidence": 0.95}

def main():
    print("Simple test bundle - main function")
    sample_data = "test_input"
    result = predict(sample_data)
    print(f"Result: {result}")

if __name__ == "__main__":
    main()