#!/bin/bash

# ../repos/qemu-5.1.0/build/qemu-img convert -O raw ow-ubu.qcow openwhisk-cache-ubu.img
# ../repos/qemu-5.1.0/build/qemu-img resize openwhisk-cache-ubu.img +16G

cd=ubuntu-20.10-live-server-amd64.iso
# drive=ow-ubu.qcow
drive=/extra/alfuerst/qemu-imgs/openwhisk-cache-ubu.img

rm -f debug.log

VMN=${VMN:=1}
# AFVMBridge0
# sudo ip addr add 192.168.223.1/24 dev AFVMBridge0
~/repos/qemu/build/x86_64-softmmu/qemu-system-x86_64 \
    -enable-kvm \
    -smp cpus=4 -cpu host \
    -m 2048M \
    -daemonize \
    -display none \
    -monitor  telnet:127.0.0.1:45682,server,nowait \
    -device virtio-net-pci,netdev=network0,mac=52:54:00:21:34:56 \
    -netdev tap,ifname=QemuTap0,id=network0,script=no \
    -drive file="$drive",if=virtio,aio=threads,format=raw \
    -debugcon file:debug.log -global isa-debugcon.iobase=0x402

    # -netdev user,id=mynet0,hostfwd=tcp:127.0.0.1:10022-:22,hostfwd=tcp::${VMN}4343-:443,hostfwd=tcp::${VMN}8080-:80 \
    # -device virtio-net-pci,netdev=mynet0 \

    # -vga none \
    # -daemonize \
    # -display none \
    # -serial none \
    # -parallel none \

    # -cdrom "$cd" \
    
