# meshmap

<p>Live web dashboard w/map for a Meshtastic node on a serial port.  Shows mesh activity and every node's position on a map, updating as packets arrive.
Node icons on the map change to green when new packet is received from that node (then age to yellow, then grey)</p>

Install:
- pip install meshtastic
- pip install pypubsub   (NOT pubsub.  That's a different, unrelated module)</li>

    
Run:

- python mesh_map.py                      # auto-detect the radio, opens http://localhost:8080
- python mesh_map.py --port COM3          # Something like COM3 for Windows.  Something like /dev/ttyUSB0 on Linux
- python mesh_map.py --tcp 192.168.1.50   # radio on Wi-Fi instead of USB
- python mesh_map.py --http-port 9000 --no-browser
- python mesh_map.py --listen 0.0.0.0     # let other devices on your LAN view the map

The map tiles come from OpenStreetMap, so the browser needs an internet connection.

Original verison of the program written by the Claude AI.  Tuned up by Ben Slade. Oct 2026
