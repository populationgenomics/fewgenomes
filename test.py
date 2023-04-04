import sys
import hailtop.batch as hb
from cpg_utils.hail_batch import get_config, remote_tmpdir

config = get_config()


def main(name: str):

    b = hb.Batch(
        name='test',
        backend=hb.ServiceBackend(
            billing_project=config['hail']['billing_project'],
            remote_tmpdir=remote_tmpdir(),
        ),
    )

    job = b.new_job('print my name')

    job.command(f'echo "{name}"')

    b.run(wait=False)


if __name__ == '__main__':
    main(sys.argv[1])
