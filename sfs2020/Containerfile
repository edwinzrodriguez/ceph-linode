#    env TMPDIR=/data/tmp podman build -t ceph:edrodrig -f Containerfile .
#    podman run ceph:edrodrig /bin/bash
#    podman ps -l
#    podman commit e6736900b8ec quay.ceph.io/edrodrig/ceph-v20.2.0
#    env TMPDIR=/data/tmp podman push quay.ceph.io/edrodrig/ceph-v20.2.0
#
#    podman pull quay.ceph.io/edrodrig/ceph-v20.2.0
#    sudo cephadm --image quay.ceph.io/edrodrig/ceph-v20.2.0 shell
#
#    sudo ceph orch upgrade start --image quay.ceph.io/edrodrig/ceph-v20.2.0:latest


ARG BASE_IMG=quay.io/ceph/ceph:v20.2.0
FROM ${BASE_IMG}
# install ceph debuginfo packages, gdb and other potentially useful packages
RUN rpm --import 'https://download.ceph.com/keys/release.asc' &&  \
    rpm -Uvh --force https://download.ceph.com/rpm-20.2.0/el9/noarch/ceph-release-1-1.el9.noarch.rpm && \
    dnf install --enablerepo='*debug*' -y \
    ceph-debuginfo \
    kernel-debuginfo \
    gdb \
    zstd \
    strace \
    perf \
    python3-debuginfo \
    perl-open.noarch \
    ceph-debuginfo \
    ceph-debugsource \
    ceph-mds-debuginfo \
    ceph-base-debuginfo \
    ceph-common-debuginfo \
    ceph-exporter-debuginfo \
    ceph-fuse-debuginfo \
    ceph-immutable-object-cache-debuginfo \
    ceph-mgr-debuginfo \
    ceph-mon-client-nvmeof-debuginfo \
    ceph-mon-debuginfo \
    ceph-osd-debuginfo \
    ceph-radosgw-debuginfo \
    ceph-test-debuginfo \
    cephfs-mirror-debuginfo \
    libcephfs-daemon-debuginfo \
    libcephfs-proxy2-debuginfo \
    libcephfs2-debuginfo \
    libcephsqlite-debuginfo \
    librados-devel-debuginfo \
    librados2-debuginfo \
    libradosstriper1-debuginfo \
    librbd1-debuginfo \
    librgw2-debuginfo \
    python3-cephfs-debuginfo \
    python3-rados-debuginfo \
    python3-rbd-debuginfo \
    python3-rgw-debuginfo \
    rbd-fuse-debuginfo \
    rbd-mirror-debuginfo \
    rbd-nbd-debuginfo \
    systemtap \
    systemtap-runtime \
    kernel-devel \
    kernel-debuginfo-common-x86_64 \
    kernel-debug && \
    /usr/bin/stap-prep
