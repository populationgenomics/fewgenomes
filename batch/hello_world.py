#!/usr/bin/env python3

import argparse
import hailtop.batch as hb


def main(args):
    examples = [
        hello_world,
        two_jobs,
        implicit_dependency,
        scatter,
        scatter_gather,
        file_gather,
        nested_scatter,
        nested_scatter_functions,
        input_file,
        output_file,
        python_job,
    ]

    if args.list or not args.example:
        help(examples)
        return

    try:
        if args.example:
            n = args.example - 1
            examples[n]()
    except:
        help(examples)


def help(examples):
    print(f'What example do you want to run?')
    for i, x in enumerate(examples):
        print(f'{i+1: >3}) {x.__name__}')

    print(f'Provide a number [1-{len(examples)}] as an argument to run')


def hello_world():
    print('Your first hail batch job')
    b = hb.Batch()
    j = b.new_job(name='hello')
    j.command('echo "hello world"')
    b.run()


def two_jobs():
    print('Run two jobs')
    b = hb.Batch(name='hello-parallel')
    s = b.new_job(name='j1')
    s.command('echo "hello world 1"')
    t = b.new_job(name='j2')
    t.command('echo "hello world 2"')

    # This makes job t dependent on s
    t.depends_on(s)

    b.run()


def implicit_dependency():
    b = hb.Batch(name='hello-serial')
    s = b.new_job(name='j1')
    s.command(f'echo "hello world" > {s.ofile}')
    t = b.new_job(name='j2')

    # Implicite dependency
    t.command(f'cat {s.ofile}')
    b.run()


def scatter():
    b = hb.Batch(name='scatter')
    for name in ['Alice', 'Bob', 'Dan']:
        j = b.new_job(name=name)
        j.command(f'echo "hello {name}"')
    b.run()


def scatter_gather():
    b = hb.Batch(name='scatter-gather-1')
    jobs = []
    for name in ['Alice', 'Bob', 'Dan']:
        j = b.new_job(name=name)
        j.command(f'echo "hello {name}"')
        jobs.append(j)
    sink = b.new_job(name='sink')
    sink.command(f'echo "I wait for everyone"')
    sink.depends_on(*jobs)
    b.run()


def file_gather():
    b = hb.Batch(name='scatter-gather-2')
    jobs = []
    for name in ['Alice', 'Bob', 'Dan']:
        j = b.new_job(name=name)
        j.command(f'echo "hello {name}" > {j.ofile}')
        jobs.append(j)
    sink = b.new_job(name='sink')
    sink.command('cat {}'.format(' '.join([j.ofile for j in jobs])))
    b.run()


def nested_scatter():
    b = hb.Batch(name='nested-scatter-1')
    for user in ['Alice', 'Bob', 'Dan']:
        for chore in ['make-bed', 'laundry', 'grocery-shop']:
            j = b.new_job(name=f'{user}-{chore}')
            j.command(f'echo "user {user} is doing chore {chore}"')
    b.run()


def do_chores(b, head, user):
    chores = []
    for chore in ['make-bed', 'laundry', 'grocery-shop']:
        j = b.new_job(name=f'{user}-{chore}')
        j.command(f'echo "user {user} is doing chore {chore}"')
        j.depends_on(head)
        chores.append(j)
    sink = b.new_job(name=f'{user}-sink')
    sink.depends_on(*chores)
    return sink


def nested_scatter_functions():
    b = hb.Batch(name='nested-scatter-3')
    head = b.new_job(name='head')
    user_sinks = []
    for user in ['Alice', 'Bob', 'Dan']:
        user_sink = do_chores(b, head, user)
        user_sinks.append(user_sink)
    final_sink = b.new_job(name='final-sink')
    final_sink.depends_on(*user_sinks)
    b.run()


def input_file():
    b = hb.Batch(name='hello-input')
    input = b.read_input('hello.txt')
    j = b.new_job(name='hello')
    j.command(f'cat {input}')
    b.run()


def output_file():
    b = hb.Batch(name='hello-input')
    j = b.new_job(name='hello')
    j.command(f'echo "hello" > {j.ofile}')
    b.write_output(j.ofile, 'output/hello.txt')
    b.run()


def hello_world(name):
    return f'hello {name}'


def upper(s):
    return s.upper()


def python_job():
    b = hb.Batch(name='python-job')
    j = b.new_python_job(name='hello')
    hello_str = j.call(hello_world, 'alice')
    result = j.call(upper, hello_str)
    b.write_output(result.as_str(), 'output/alice.txt')
    b.run()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Run simple hail batch examples'
    )
    parser.add_argument(
        '--example',
        metavar='N',
        type=int,
        help='example number that you want to run',
    )
    parser.add_argument(
        '--list',
        action='store_true',
        default=False,
        help='List all possible examples',
    )

    args = parser.parse_args()
    main(args)
