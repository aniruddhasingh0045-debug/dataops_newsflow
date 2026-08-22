import pytest
import pandas as pd

def test_quality_check_detects_null_urls():
    df = pd.DataFrame({
        'title': ['Article 1', 'Article 2'],
        'url': ['http://example.com/1', None],
        'source': ['Source A', 'Source B']
    })
    
    with pytest.raises(ValueError):
        if not df['url'].notna().all():
            raise ValueError("Null URLs found")

def test_quality_check_passes_valid_data():
    df = pd.DataFrame({
        'title': ['Article 1', 'Article 2', 'Article 3'],
        'url': ['http://example.com/1', 'http://example.com/2', 'http://example.com/3'],
        'source': ['Source A', 'Source B', 'Source C']
    })
    
    assert df['url'].notna().all()
    assert len(df) > 0

def test_quality_check_detects_too_few_articles():
    df = pd.DataFrame({
        'title': ['Article 1'],
        'url': ['http://example.com/1'],
        'source': ['Source A']
    })
    
    with pytest.raises(ValueError):
        if len(df) <= 10:
            raise ValueError(f"Too few articles: {len(df)}")
