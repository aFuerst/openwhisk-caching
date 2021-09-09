#!/bin/bash

# ../repos/qemu-5.1.0/build/qemu-img convert -O raw ow-ubu.qcow openwhisk-cache-ubu.img
# ../repos/qemu-5.1.0/build/qemu-img resize openwhisk-cache-ubu.img +16G

drive=/extra/alfuerst/qemu-imgs/openwhisk-invoker-ubu.img
drive=/extra/alfuerst/qemu-imgs/openwhisk-cache-ubu.qcow2

# qemu-img convert -f raw -O qcow2 /extra/alfuerst/qemu-imgs/openwhisk-cache-ubu.img /extra/alfuerst/qemu-imgs/openwhisk-cache-ubu.qcow2

rm -f invoker-*-debug.log

VMN=2
macaddr="06:01:02:03:04:0$VMN"
net="mynet$VMN"
debuglog="invoker-$VMN-debug.log"
telnet="127.0.0.1:1568$VMN"
echo $macaddr
echo $net
echo $drive
echo $debuglog

# ~/repos/qemu/build/x86_64-softmmu/qemu-system-x86_64
qemu-system-x86_64 \
    -enable-kvm \
    -smp cpus=12 -cpu host \
    -m 24G \
    -daemonize \
    -nographic \
    -display none \
    -netdev bridge,id=$net,br=br0 \
    -device virtio-net-pci,netdev=$net,mac=$macaddr \
    -monitor telnet:$telnet,server,nowait \
    -drive file="$drive",if=virtio,aio=threads,format=raw \
    -debugcon file:$debuglog -global isa-debugcon.iobase=0x402


# for invokerid in 5 6 7
# do

# mac="06:01:02:03:04:0$invokerid"
# tel="127.0.0.1:4568$invokerid"
# guestimg="/data/alfuerst/qemu-vms/backing-files/openwhisk-guest-$invokerid.qcow2"
# debug="debug-$invokerid.log"
# net="mynet$invokerid"

# rm -f $guestimg
# rm -f $debug

# qemu-img create -f qcow2 \
#                 -o backing_file=$drive \
#                 $guestimg

# qemu-system-x86_64 \
#     -enable-kvm \
#     -smp cpus=12 -cpu host \
#     -m 24G \
#     -daemonize \
#     -nographic \
#     -display none \
#     -monitor  telnet:$tel,server,nowait \
#     -netdev bridge,id=$net,br=br0 \
#     -device virtio-net-pci,netdev=$net,mac=$macaddr \
#     -drive file="$guestimg",if=virtio,aio=threads,format=qcow2 \
#     -debugcon file:$debug -global isa-debugcon.iobase=0x402

# echo ""

# done
