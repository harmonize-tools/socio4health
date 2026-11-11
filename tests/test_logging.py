import logging
import zipfile

import pytest

import socio4health
from socio4health import Extractor
from socio4health.utils.log import logger


@pytest.fixture(autouse=True)
def _restore_level():
    level = logger.level
    yield
    logger.setLevel(level)


def test_package_does_not_configure_root_logger():
    assert logging.getLogger().handlers == [] or all(
        "socio4health" not in type(h).__module__ for h in logging.getLogger().handlers
    )
    assert logger.propagate is False


def test_info_goes_to_stdout_and_warnings_to_stderr(capsys):
    logger.info("informative message")
    logger.warning("something is wrong")
    out, err = capsys.readouterr()
    assert "informative message" in out and "informative message" not in err
    assert "something is wrong" in err and "something is wrong" not in out


def test_set_log_level_silences_info(capsys):
    socio4health.s4h_set_log_level("WARNING")
    logger.info("hidden")
    assert "hidden" not in capsys.readouterr().out


@pytest.fixture
def zipped_census(tmp_path):
    src = tmp_path / "src"
    src.mkdir()
    for state in ["AC", "AL", "AM"]:
        with zipfile.ZipFile(src / f"{state}.zip", "w") as z:
            z.writestr(f"{state}/{state}_Domicilios.txt", "1201\n1202\n")
            z.writestr(f"{state}/{state}_Pessoas.txt", "12XX\n")
    return tmp_path


def test_extraction_prints_one_summary_per_step_not_one_line_per_file(zipped_census, capsys):
    ddfs = Extractor(input_path=str(zipped_census / "src"), down_ext=[".zip", ".txt"],
                     output_path=str(zipped_census / "out"), is_fwf=True,
                     colnames=["UF", "N"], colspecs=[(0, 2), (2, 4)]).s4h_extract()
    out, err = capsys.readouterr()

    assert len(ddfs) == 6
    assert "Extracted 6 files from 3 archives" in out
    assert "Read 6/6 files" in out
    assert "Extracted: " not in out  # per-file messages are DEBUG now
    assert err == ""                 # nothing in red in a notebook


def test_local_mode_extracts_each_archive_once(zipped_census):
    ddfs = Extractor(input_path=str(zipped_census / "src"), down_ext=[".zip", ".7z", ".txt"],
                     output_path=str(zipped_census / "out"), is_fwf=True,
                     colnames=["UF", "N"], colspecs=[(0, 2), (2, 4)]).s4h_extract()
    assert len(ddfs) == 6
