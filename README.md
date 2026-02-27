- Neey Balancer Readout with Raspberry PI 
I was looking for a solution to use my existing raspberry pi to readout a neey balancer over ble without an additional esp32.
A perfect starting point was the code  of  esphome-jk-bms
https://github.com/syssi/esphome-jk-bms
With the help of 3 differerent AI Tools (Deepwiki, Kimi, Claude) i got a good working 
solution with very low effort.
I asked deepwiki.com „Can you recode to Python“ of the original code
https://github.com/syssi/esphome-jk-bms/blob/112f98f7/components/heltec_balancer_ble/heltec_balancer_ble.cpp
https://deepwiki.com/search/can-you-recode-to-python_e17de732-e23e-45e7-a314-dcdbea442055?mode=deep
which gave me a working python test solution of the original code

now i put this python code in https://kimi.com 

prompt the ai tool add  MQTT Support, Install-tips as a service

i put the kimi generated code parallel  in https://claude.ai

after some fine tuning both AI-tools delivered good working code
 I created not a single line of code by hand. 
I only prompted my requirements to the tools and the error messages of  the code which clearly doesnt work perfect at the first time, but after some iterations i had working code tailored to my needs.


