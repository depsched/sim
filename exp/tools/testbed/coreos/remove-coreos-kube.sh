#!/bin/bash

for vm in $(VBoxManage list vms | sed "s/\"\(.*\)\".*/\1/" | grep vagrant); do vboxmanage controlvm $vm poweroff; vboxmanage unregistervm --delete $vm; done
