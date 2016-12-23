#!/usr/bin/expect #Where the script should be run from.

set ip [lindex $argv 0]

#If it all goes pear shaped the script will timeout after 20 seconds.
set timeout 20
#First argument is assigned to the variable name
set name route-views.routeviews.org
#Second argument is assigned to the variable user
set user rviews
#Third argument is assigned to the variable password
set password rviews
#This spawns the telnet program and connects it to the variable name
spawn telnet $name 
#The script expects login
expect "Username:" 
#The script sends the user variable
send "$user\r"

send "terminal length 0\r"
send "show ip bgp $ip\r"
send "exit\r"

#This hands control of the keyboard over two you (Nice expect feature!)
interact

exit
