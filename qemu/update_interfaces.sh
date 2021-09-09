pth="/etc/network/interfaces"
bak="/etc/network/interfaces.bak"

cp $pth $bak

cp "./interfaces" $pth

/etc/init.d/networking restart

echo "Waiting to test network setup"
sleep 60s
echo "Done waiting, reverting"

cp $bak $pth
/etc/init.d/networking restart

echo ""
