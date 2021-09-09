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
# ~/repos/qemu/build/x86_64-softmmu/qemu-system-x86_64
# ~/repos/qemu/build/x86_64-softmmu/

# will be using IP addr 172.29.200.161
qemu-system-x86_64 \
    -enable-kvm \
    -smp cpus=24 -cpu host \
    -m 64G \
    -daemonize \
    -nographic \
    -display none \
    -netdev bridge,id=mynet0,br=br0 \
    -device virtio-net-pci,netdev=mynet0,mac=06:01:02:03:04:00 \
    -monitor telnet:127.0.0.1:45682,server,nowait \
    -drive file="$drive",if=virtio,aio=threads,format=raw \
    -debugcon file:debug.log -global isa-debugcon.iobase=0x402

  #,hostfwd=tcp::10022-:22,hostfwd=tcp::${VMN}4343-:443,hostfwd=tcp::${VMN}8080-:80 \

    # -netdev tap,id=n1,br="br0",script="/etc/qemu-ifup",downscript="/etc/qemu-ifdown" \
    # -device virtio-net,netdev=n1,mac="52:54:98:76:54:32" \


    # -netdev user,id=net0 \
    # -device virtio-net,netdev=net0,mac=06:01:02:03:04:0F \

    # -daemonize \
    # -nographic \
    # -display none \

    # -device virtio-net-pci,netdev=net0,mac=06:01:02:03:04:0F \
    # -netdev bridge,id=net0 \


    # -netdev user,id=mynet1,hostfwd=tcp:127.0.0.1:10022-:22,hostfwd=tcp::${VMN}4343-:443,hostfwd=tcp::${VMN}8080-:80 \
    # -device virtio-net-pci,netdev=mynet1 \

    # -device virtio-net-pci,netdev=net0,mac=06:01:02:03:04:00 \
    # -netdev tap,ifname=QemuTap0,id=net0,script=no \

    # -device virtio-net-pci,netdev=net0,mac=06:01:02:03:04:00 \
    # -netdev bridge,br=afbr0,id=net0 \

    # -net nic,model=virtio,macaddr=06:01:02:03:04:00 \
    # -net bridge \

    # -vga none \
    # -daemonize \
    # -display none \
    # -serial none \
    # -parallel none \

    # -cdrom "$cd" \
    
