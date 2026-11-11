import zipfile

import pytest

from socio4health import Extractor


def test_extensions_are_normalized():
    ext = Extractor(input_path="http://example.invalid/", down_ext=[".CSV", ".csv", "zip", " .Zip "])
    assert ext.down_ext == [".csv", ".zip"]


def test_local_mode_matches_extensions_in_any_case(tmp_path):
    src = tmp_path / "src"
    (src / "sub").mkdir(parents=True)
    (src / "upper.CSV").write_text("a;b\n1;2\n")
    (src / "sub" / "lower.csv").write_text("a;b\n3;4\n")
    (src / "ignored.txt").write_text("x\n")
    with zipfile.ZipFile(src / "ARCHIVE.ZIP", "w") as z:
        z.writestr("inside.Csv", "a;b\n5;6\n")

    ddfs = Extractor(input_path=str(src), down_ext=[".csv", ".zip"], sep=";",
                     output_path=str(tmp_path / "out")).s4h_extract()

    names = sorted(d["filename"].compute().iloc[0].split("_", 1)[-1] for d in ddfs)
    assert names == ["inside.Csv", "lower.csv", "upper.CSV"]


def test_local_mode_ignores_cache_and_partial_files(tmp_path):
    src = tmp_path / "src"
    (src / "data.txt.parquet").mkdir(parents=True)
    (src / "data.txt.parquet" / "part.0.parquet").write_bytes(b"not a real parquet")
    (src / "data.txt.parquet.json").write_text("{}")
    (src / "real.json.part").write_text("{")

    ext = Extractor(input_path=str(src), down_ext=[".parquet", ".json"], output_path=str(tmp_path / "out"))
    assert ext.s4h_extract() == []


def test_spider_matches_links_in_any_case():
    pytest.importorskip("scrapy")
    from scrapy.http import HtmlResponse
    from socio4health.utils.standard_spider import StandardSpider

    html = b"""<html><body>
        <a href="/files/DATA_2022.CSV">a</a>
        <a href="/files/other.csv">b</a>
        <a href="/files/archive.ZIP">c</a>
        <a href="/files/readme.pdf">d</a>
    </body></html>"""
    response = HtmlResponse(url="https://example.org/catalog", body=html, encoding="utf-8",
                            headers={"Content-Type": "text/html"})

    spider = StandardSpider(url="https://example.org/catalog", depth=0, ext=[".csv", ".zip"])
    list(spider.parse(response))

    assert sorted(spider.links) == ["DATA_2022.CSV", "archive.ZIP", "other.csv"]
