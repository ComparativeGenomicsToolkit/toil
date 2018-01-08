"""
Un-inline large files from an AWSJobStore.
"""

from argparse import ArgumentParser
from toil.jobStores.aws.jobStore import AWSJobStore

parser = ArgumentParser()
parser.add_argument('jobStore')
opts = parser.parse_args()

jobStore = AWSJobStore(opts.jobStore)
jobStore._bind(create=False)
largeItems = map(lambda x: x.name, jobStore.filesDomain.select('select itemName() from `' + jobStore.filesDomain.name + '` where `003` is not null'))
for itemName in largeItems:
    print itemName
    info = jobStore.FileInfo.load(itemName)
    assert info.content is not None
    content = info.content
    info._content = None
    with info.uploadStream() as f:
        f.write(content)
    info.save()
