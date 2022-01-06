import hailtop.batch as hb


"""
here's some playing around with hail batch
"""


# # create a new hail batch and run a single job
# bat = hb.Batch(name="pinky")
# job1 = bat.new_job(name="a")
# job1.command('echo "who da man"')
# bat.run()

# # make two jobs that run independently
# bat = hb.Batch(name="pinky")
# job1 = bat.new_job(name="a")
# job1.command('echo "who da man"')
# job2 = bat.new_job(name="b")
# job2.command('echo "who da man 2"')
# bat.run()

# # make two jobs where the order is determined by a dependency
# bat = hb.Batch(name="pinky")
# job1 = bat.new_job(name="a")
# job1.command('echo "who da man"')
# job2 = bat.new_job(name="b")
# job2.command('echo "who da man 2"')
# job1.depends_on(job2)
# bat.run()

# # add a file dependency, so one job writes to a file and another reads the same
# bat = hb.Batch(name="pinky")
# job1 = bat.new_job(name="a")
# job1.command(f'echo "who da man" > {job1.ofile}')  # ofile = output file
# job2 = bat.new_job(name="b")
# job2.command(f'cat {job1.ofile}')
# bat.run()

# # we can use a loop to create a number of jobs
# bat = hb.Batch(name='loopy')
# for name in ['a', 'b', 'c']:
#     j = bat.new_job(name=name)
#     j.command(f'echo "my name is {name}"')
# bat.run()

# # scatter and sink - we can iterate and create dependencies
# bat = hb.Batch(name="MapReduce")
# jobs = []
# for name in ['a', 'b', 'c']:
#     j = bat.new_job(name=name)
#     j.command(f'echo "my name is {name}"')
#     jobs.append(j)
# sink = bat.new_job(name='sink')
# sink.command(f'echo "I waited for {[j.name for j in jobs]}"')
# sink.depends_on(*jobs)
# bat.run()

# # scatter - we can add dependencies to an existing job
# bat = hb.Batch(name="MapReduce")
# sink = bat.new_job(name='sink')
# sink.command(f'echo "I am the watcher"')
# for name in ['a', 'b', 'c']:
#     j = bat.new_job(name=name)
#     j.command(f'echo "my name is {name}" && sleep 1')
#     sink.depends_on(j)
# bat.run()

# # scatter 2 - we can
# bat = hb.Batch(name="MapReduce-with-files")
# jobs = []
# for name in ['a', 'b', 'c']:
#     j = bat.new_job(name=name)
#     j.command(f'echo "my name is {name}" > {j.ofile}')
#     jobs.append(j)
#
# sink = bat.new_job(name='sink-2')
# sink.command('cat {}'.format(' '.join([j.ofile for j in jobs])))
# sink.depends_on(*jobs)
# bat.run()

# ok, and now a more complex structure with checkpoint steps
"""
e.g.
for each batch of files, run a full separate process for each
then upon completion of all groups, perform some operation on all outputs 
"""


def create_group(batch, name, head, joblist=('clean', 'aids')):
    """
    the batch to add to
    the name for this sub-batch
    the top level job to start from (gotta start the graph somewhere
    """
    sink_job = batch.new_job(f'sink_{name}')

    for task in joblist:
        j = batch.new_job(name=f"{name}-{task}")
        j.command(f'echo "{name}-{task}"; sleep 4')
        j.depends_on(head)
        sink_job.depends_on(j)
    return sink_job


bat = hb.Batch(name='top')
head = bat.new_job('head')
all_sinks = []
for name in ['a', 'b', 'c']:
    all_sinks.append(create_group(bat, name, head))
final_job = bat.new_job(name="final")
final_job.depends_on(*all_sinks)
bat.run()

