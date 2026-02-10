import setuptools

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setuptools.setup(
    name="ceph-linode",
    version="0.1.0",
    author="batrick",
    description="Scripts to deploy Ceph in Linode and IBM Cloud",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/batrick/ceph-linode",
    packages=setuptools.find_packages(),
    py_modules=["linode", "ibmcloud"],
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.6',
    install_requires=[
        "linode_api4",
        "notario",
        "PyYAML",
        "requests",
        "ibm-vpc",
        "ibm-cloud-sdk-core",
        "ibm-platform-services",
    ],
    entry_points={
        "console_scripts": [
            "ceph-linode=linode:main",
            "ceph-ibmcloud=ibmcloud:main",
            "cephfs-perf-test=sfs2020.cephfs_perf_test:main",
            "perf-record=sfs2020.perf_record:main",
            "run-workload=sfs2020.run_workload:main",
        ],
    },
)
