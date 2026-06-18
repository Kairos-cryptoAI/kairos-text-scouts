from kairos_text.filter import LocalRelevanceFilter
from kairos_text.models import NewsItem


def test_drops_noise_keeps_relevant():
    items = [
        NewsItem(title="SEC approves spot Bitcoin ETF", source="reuters"),
        NewsItem(title="Local bakery wins award", source="local"),
        NewsItem(title="Fed signals rate cut as CPI inflation cools", source="bloomberg"),
        NewsItem(title="Celebrity buys a yacht", source="tabloid"),
    ]
    kept = LocalRelevanceFilter(threshold=3.0, top_k=5).select(items)
    titles = [i.title for i in kept]
    assert any("ETF" in t for t in titles)
    assert any("CPI" in t for t in titles)
    assert all("bakery" not in t and "yacht" not in t for t in titles)


def test_top_k_caps_output():
    items = [NewsItem(title=f"Bitcoin ETF approval news {i}") for i in range(20)]
    assert len(LocalRelevanceFilter(top_k=5).select(items)) == 5
