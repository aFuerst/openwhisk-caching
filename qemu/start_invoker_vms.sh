#!/bin/bash

# ../repos/qemu-5.1.0/build/qemu-img convert -O raw ow-ubu.qcow openwhisk-cache-ubu.img
# ../repos/qemu-5.1.0/build/qemu-img resize openwhisk-cache-ubu.img +16G

drive=/extra/alfuerst/qemu-imgs/openwhisk-invoker-ubu.img

rm -f debug.log

VMN=1
~/repos/qemu/build/x86_64-softmmu/qemu-system-x86_64 \
    -enable-kvm \
    -smp cpus=12 -cpu host \
    -m 24G \
    -daemonize \
    -display none \
    -monitor  telnet:127.0.0.1:${VMN}5682,server,nowait \
    -device virtio-net-pci,netdev=network0,mac=52:54:00:21:34:56 \
    -netdev tap,ifname=QemuTap1,id=network0,script=no \
    -drive file="$drive",if=virtio,aio=threads,format=raw \
    -debugcon file:debug.log -global isa-debugcon.iobase=0x402

    # -netdev user,id=mynet0,hostfwd=tcp::${VMN}0022-:22,hostfwd=tcp::${VMN}4343-:443,hostfwd=tcp::${VMN}5080-:80,hostfwd=tcp::1200${VMN}-:1200${VMN} \
    # -device virtio-net-pci,netdev=mynet0 \


# for invokerid in 1 2 3 4
# do

# VMN=$invokerid
# ~/repos/qemu/build/x86_64-softmmu/qemu-system-x86_64 \
#     -enable-kvm \
#     -smp cpus=12 -cpu host \
#     -m 24G \
#     -daemonize \
#     -display none \
#     -monitor  telnet:127.0.0.1:${VMN}5682,server,nowait \
#     -netdev user,id=mynet0,hostfwd=tcp:127.0.0.1:${VMN}0022-:22,hostfwd=tcp::${VMN}4343-:443,hostfwd=tcp::${VMN}5080-:80 \
#     -device virtio-net-pci,netdev=mynet0 \
#     -drive file="$drive",if=virtio,aio=threads,format=raw \
#     -debugcon file:debug.log -global isa-debugcon.iobase=0x402

# done

