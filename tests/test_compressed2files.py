import io
import os
import tarfile
import zipfile
from unittest.mock import patch

import pytest

from socio4health.utils.extractor_utils import compressed2files


@pytest.fixture
def survey_zip(tmp_path):
    """Like DANE's GEIH: the same data as CSV, DTA and SAV, plus a nested zip."""
    inner = io.BytesIO()
    with zipfile.ZipFile(inner, "w") as z:
        z.writestr("inner/extra.csv", "a;b\n1;2\n")
        z.writestr("inner/extra.dta", b"\0" * 1000)

    path = tmp_path / "GEIH.zip"
    with zipfile.ZipFile(path, "w", zipfile.ZIP_DEFLATED) as z:
        z.writestr("GEIH/CSV/Ocupados.csv", "a;b\n1;2\n")
        z.writestr("GEIH/CSV/Hogar.CSV", "a;b\n3;4\n")
        z.writestr("GEIH/DTA/Ocupados.dta", b"\0" * 100_000)
        z.writestr("GEIH/SAV/Ocupados.sav", b"\0" * 100_000)
        z.writestr("GEIH/nested.zip", inner.getvalue())
    return path


def _names(files):
    return sorted(os.path.basename(f).split("_", 1)[1] for f in files)


def test_only_wanted_extensions_are_written(survey_zip, tmp_path):
    out = tmp_path / "out"
    files = compressed2files(str(survey_zip), str(out), [".csv", ".CSV", ".zip"])

    assert _names(files) == ["GEIH_CSV_Hogar.CSV", "GEIH_CSV_Ocupados.csv", "inner_extra.csv"]
    on_disk = sorted(os.listdir(out))
    assert len(on_disk) == 3
    assert not any(n.endswith((".dta", ".sav", ".part")) for n in on_disk)


def test_already_extracted_members_are_skipped(survey_zip, tmp_path):
    out = tmp_path / "out"
    first = compressed2files(str(survey_zip), str(out), [".csv"])

    real_copy = __import__("shutil").copyfileobj
    copied = []

    def counting_copy(src, dst, *args):
        copied.append(getattr(dst, "name", "?"))
        return real_copy(src, dst, *args)

    with patch("socio4health.utils.extractor_utils.shutil.copyfileobj", side_effect=counting_copy):
        second = compressed2files(str(survey_zip), str(out), [".csv"])

    assert second == first  # nested archive names are stable between runs
    assert all(not name.endswith(".csv.part") for name in copied)  # only the nested zip is re-read


def test_tar_archives_are_filtered_too(tmp_path):
    path = tmp_path / "data.tar.gz"
    with tarfile.open(path, "w:gz") as tar:
        for name, data in [("d/a.csv", b"x,y\n1,2\n"), ("d/a.sav", b"\0" * 5000)]:
            info = tarfile.TarInfo(name)
            info.size = len(data)
            tar.addfile(info, io.BytesIO(data))

    files = compressed2files(str(path), str(tmp_path / "out"), [".csv"])

    assert _names(files) == ["d_a.csv"]
    assert os.listdir(tmp_path / "out") == [os.path.basename(next(iter(files)))]
