import click
import json
import pandas
import pandas as pd
import requests
import subprocess

from csv import DictReader
from io import StringIO
from requests.structures import CaseInsensitiveDict


"""
this is a requests-centric process which takes a number of Family IDs, and returns a JSON string
the output string contains a dictionary of each family ID to a set of identifiers
by default these identifiers are CPG internal sample IDs
by argument these can be external IDs instead 

This is a lil script for finding samples belonging in each family. This would be faster and more robust as a DB query
if this turns out to be useful to anyone, the same function should be recreated in the sample-metadata api repo
"""


def get_auth() -> str:
    """
    uses a shell to execute the auth token grab
    :return: str: the token generated by the gcloud process

    This is of course a greasy hack, and will never translate to any other environments
    It does facilitate local playing with the metadata api, and that will broadly translate into other contexts
    """
    return subprocess.Popen(
        ['gcloud', 'auth', 'print-identity-token'],
        stdout=subprocess.PIPE,
        universal_newlines=True
    ).communicate()[0].rstrip()


def get_response(url, headers, query_params=None) -> requests.Response:
    """
    perform the actual execution of the URL with provided parameters
    :param url: str
    :param headers: dict
    :param query_params: [optional] dict
    :return: requests.Response
    """

    resp = requests.get(headers=headers, url=url, params=query_params)
    if resp.status_code != 200:
        raise Exception(f"non-200 status code for {url}, {headers}, {query_params}")

    return resp


def get_family_to_sample_map(df: pandas.DataFrame, families: str, external: bool, pid_to_id: dict, int_to_ext: dict
                             ) -> dict:
    """
    take an arb. number of families from the command line, and for each, find all sample IDs. Return as a JSON dict
    :param df: the pedigrees dataframe
    :param families: str, a comma-delimited string containing family IDs
    :param external: bool, True if we want to return external sample IDs, false for CPG IDs
    :param pid_to_id: dict, lookup of int. participant ID to int. sample ID
    :param int_to_ext: dict, lookup of int. sample ID to external

    NOTE; this is all assuming a single participant-sample connection in the API results
    """

    dict_result = {}

    for family_id in families.split(","):

        dict_result[family_id] = []

        # isolate all rows containing the family members from the pedigree table
        members_df = df.loc[df["Family ID"] == family_id]
        original_ids = {row["Individual ID"] for index, row in members_df.iterrows()}

        # translate those IDs to something usable
        for member_id in original_ids:

            # participant to sample
            int_sample_id = pid_to_id.get(member_id)

            # internal sample to external sample
            ext_sample_id = int_to_ext.get(int_sample_id)

            # store the appropriate value, depending on argument flags
            if external:
                dict_result[family_id].append(ext_sample_id)
            else:
                dict_result[family_id].append(int_sample_id)

    return dict_result


@click.command()
@click.option(
    "--project",
    "project",
    type=click.STRING,
    help="the name of the project to use in API queries"
)
@click.option(
    "--families",
    "families",
    type=click.STRING,
    help="a comma-delimited string, containing all the family IDs to search for"
)
@click.option(
    "--external",
    "external",
    type=click.BOOL,
    default=False,
    is_flag=True,
    help="if this is set, return external IDs instead"
)
def main(project: str, families: str, external: bool):
    # only get this once, for now
    auth_token = get_auth()

    # currently the 2 types of endpoint require different headers, with the same auth token
    table_header = CaseInsensitiveDict(data={"Accept": ACCEPT_ALL, "Authorization": f"Bearer {auth_token}"})
    mapping_header = CaseInsensitiveDict(data={"Accept": ACCEPT_JSON, "Authorization": f"Bearer {auth_token}"})

    # core query params from documentation
    # get the pedigrees across the entire project
    response = get_response(
        url=f"{URL_BASE}/{FAMILY_ENDPOINT}/{project}/{PEDIGREE}",
        headers=table_header,
        query_params={
            "replace_with_participant_external_ids": True,
            "replace_with_family_external_ids": True,  # False
            "empty_participant_value": "",
            "include_header": True
        }
    )

    # parse this into a series of dictionaries, then into a dataframe
    dict_read = DictReader(StringIO(response.content.decode().strip("#")), delimiter="\t")
    pedigree_df = pd.DataFrame([line for line in dict_read])

    # -#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#- #
    # so now we have a dataframe of the entire pedigree structure
    # NOTE - this is just a hack around, with examples of further hackery

    # # example queries on pedigree_df - all affected females
    # aff_females = pedigree_df.loc[(pedigree_df["Affected"] == "2") & (pedigree_df["Sex"] == "2")]
    #
    # # all members of those families
    # all_aff_female_families = pedigree_df.loc[pedigree_df["Family ID"].isin(aff_females["Family ID"].values)]
    #
    # # are there any affected parents? - affected and not "proband" in name (should be none in acute care)
    # aff_parents = pedigree_df.loc[
    #   (pedigree_df["Affected"] == "2") &
    #   (~pedigree_df["Individual ID"].str.contains("proband")),
    #   ["Family ID", "Individual ID"]
    # ]
    # -#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#- #

    # we can obtain a lookup on the internal sample ID using the external PID
    # participant/acute-care/external-pid-to-internal-sample-id # JSON
    # this will get us a CPG ID (INTernal, not INTeger)
    response = get_response(url=f"{URL_BASE}/{PARTICIPANT_ENDPOINT}/{project}/external-pid-to-internal-sample-id",
                            headers=mapping_header)
    paired_samples_list = response.json()
    pid_to_sample_id = dict(zip([_[0] for _ in paired_samples_list], [_[1] for _ in paired_samples_list]))

    # we can then map the internal sample IDs to external sample IDs
    # sample/acute-care/id-map/internal/all # JSON
    response = get_response(url=f"{URL_BASE}/{SAMPLE_ENDPOINT}/{project}/id-map/internal/all", headers=mapping_header)
    internal_to_ext_map = response.json()

    family_to_samples_dict = get_family_to_sample_map(
        df=pedigree_df,
        families=families,
        external=external,
        pid_to_id=pid_to_sample_id,
        int_to_ext=internal_to_ext_map
    )

    # now print as a string value, with no spaces
    print(json.dumps(family_to_samples_dict, separators=(',', ':')))


URL_BASE = "https://sample-metadata-api-mnrpw3mdza-ts.a.run.app/api/v1"
ACCEPT_ALL = "*/*"
ACCEPT_JSON = "application/json"
FAMILY_ENDPOINT = "family"
PARTICIPANT_ENDPOINT = "participant"
SAMPLE_ENDPOINT = "sample"
PEDIGREE = "pedigree"


if __name__ == "__main__":
    main()
