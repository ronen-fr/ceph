#!/bin/bash -ex

SCRIPT_NAME=$(basename ${BASH_SOURCE[0]})

fsid='00000000-0000-0000-0000-0000deadbeef'
image='ceph/daemon-base:latest-master-devel'
[ -z "$ip" ] && ip=127.0.0.1

OSD_IMAGE_NAME="${SCRIPT_NAME%.*}_osd.img"
OSD_IMAGE_SIZE='6G'
OSD_TO_CREATE=6
OSD_VG_NAME=${SCRIPT_NAME%.*}
OSD_LV_NAME=${SCRIPT_NAME%.*}

CEPH_DAEMON=../src/ceph-daemon/ceph-daemon

#A="-d"

# clean up previous run(s)?
$CEPH_DAEMON $A rm-cluster --fsid $fsid --force
vgchange -an $OSD_VG_NAME || true
loopdev=$(losetup -a | grep $(basename $OSD_IMAGE_NAME) | awk -F : '{print $1}')
if ! [ "$loopdev" = "" ]; then
    losetup -d $loopdev
fi
rm -f $OSD_IMAGE_NAME

cat <<EOF > c
[global]
log to file = true
EOF

$CEPH_DAEMON $A \
    --image $image \
    bootstrap \
    --mon-id a \
    --mgr-id x \
    --fsid $fsid \
    --mon-ip $ip \
    --config c \
    --output-keyring k \
    --output-config c \
    --allow-overwrite
chmod 644 k c

if [ -n "$ip2" ]; then
    # mon.b
    $CEPH_DAEMON $A \
    --image $image \
    deploy --name mon.b \
    --fsid $fsid \
    --mon-addrv "[v2:$ip2:3300,v1:$ip2:6789]" \
    --keyring /var/lib/ceph/$fsid/mon.a/keyring \
    --config c
fi

# mgr.b
bin/ceph -c c -k k auth get-or-create mgr.y \
	 mon 'allow profile mgr' \
	 osd 'allow *' \
	 mds 'allow *' > k-mgr.y
$CEPH_DAEMON $A \
    --image $image \
    deploy --name mgr.y \
    --fsid $fsid \
    --keyring k-mgr.y \
    --config c

# mds.{k,j}
for id in k j; do
    bin/ceph -c c -k k auth get-or-create mds.$id \
	     mon 'allow profile mds' \
	     mgr 'allow profile mds' \
	     osd 'allow *' \
	     mds 'allow *' > k-mds.$id
    $CEPH_DAEMON $A \
	--image $image \
	deploy --name mds.$id \
	--fsid $fsid \
	--keyring k-mds.$id \
	--config c
done

# add osd.{1,2,..}
dd if=/dev/zero of=$OSD_IMAGE_NAME bs=1 count=0 seek=$OSD_IMAGE_SIZE
loop_dev=$(losetup -f)
losetup $loop_dev $OSD_IMAGE_NAME
pvcreate $loop_dev && vgcreate $OSD_VG_NAME $loop_dev
for id in `seq 0 $((--OSD_TO_CREATE))`; do
    lvcreate -l $((100/$OSD_TO_CREATE))%VG -n $OSD_LV_NAME.$id $OSD_VG_NAME
    $SUDO $CEPH_DAEMON shell --config c --keyring k -- \
            ceph orchestrator osd create \
                $(hostname):/dev/$OSD_VG_NAME/$OSD_LV_NAME.$id
done

bin/ceph -c c -k k -s
