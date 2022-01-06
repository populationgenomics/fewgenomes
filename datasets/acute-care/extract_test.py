import hail as hl
import logging
import os
import pytest

# import the local method
from extract_trio_vcf import check_for_samples, get_all_unique_members


"""
this should be a companion test to make sure the syntax for the extraction is accurate
this repo doesn't have CI/a pytest/unittest framework, but this can stand alone or be a start for more tests

The VCF is a minimised example, and contains only the samples G00607 HG00619 HG00623 HG00657
The VCF also contains only one variant
"""


# until there is a broader unittest setup, this references a local file
CURDIR = os.path.dirname(os.path.abspath(__file__))
VCF_PATH = os.path.join(CURDIR, "1kg.vcf.bgz")
MT_PATH = os.path.join(CURDIR, "test_input.mt")


@pytest.fixture()
def load_mt():
    pass


# family definitions; one complete, one incomplete, one with no samples present at all
@pytest.fixture()
def partial_family_dict():
    yield {
        "fam1": ['HG00607', 'HG00619', 'HG00623'],
        "fam2": ['HG00657', "not", "present"]
    }


@pytest.fixture()
def complete_family_dict():
    yield {
        "fam1": ['HG00607', 'HG00619', 'HG00623'],
        "fam2": ['HG00657']
    }


@pytest.fixture()
def zero_family_dict():
    yield {
        "fam1": ['all', 'samples', 'are'],
        "fam2": ['missing']
    }


@pytest.fixture()
def empty_family_dict():
    yield {
        "fam1": [],
        "fam2": []
    }


def test_chain_empty(empty_family_dict: dict):
    result = get_all_unique_members(empty_family_dict)
    assert result == set()


def test_chain(zero_family_dict: dict):
    result = get_all_unique_members(zero_family_dict)
    assert sorted(result) == ["all", "are", "missing", "samples"]


class TestMT:
    """
    all tests requiring the mt to be loaded
    """

    @classmethod
    def setup_class(cls):
        """
        only commit one VCF file for tidiness - read and translate VCF to MT once
        """
        hl.import_vcf(VCF_PATH).write(MT_PATH, overwrite=True)
        cls.mt = hl.read_matrix_table(MT_PATH)
        logging.basicConfig(level=logging.INFO)

    def test_partial_failure(self, caplog, partial_family_dict):
        caplog.set_level(logging.INFO)
        all_samples = {'HG00607', 'HG00619', 'HG00623', 'HG00657', 'not', 'present'}
        bool_response = check_for_samples(
            sample_names=all_samples,
            family_structures=partial_family_dict,
            mat=self.mt
        )
        assert not bool_response
        assert "Family fam2 is not fully represented in the data. Samples missing: ['not', 'present']" in caplog.text

    def test_full_failure(self, caplog, zero_family_dict):
        caplog.set_level(logging.INFO)
        all_samples = {"all", "are", "missing", "samples"}
        bool_response = check_for_samples(
            sample_names=all_samples,
            family_structures=zero_family_dict,
            mat=self.mt
        )
        assert not bool_response
        # can't predict the sample that will be popped off
        assert "No requested samples were present in the MT, please check format matches 'HG" in caplog.text

    def test_working_check(self, caplog: pytest.fixture, complete_family_dict: dict):
        caplog.set_level(logging.INFO)
        all_samples = {'HG00607', 'HG00619', 'HG00623', 'HG00657'}
        bool_response = check_for_samples(
            sample_names=all_samples,
            family_structures=complete_family_dict,
            mat=self.mt
        )
        assert bool_response
