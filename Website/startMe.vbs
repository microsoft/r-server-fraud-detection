user = WScript.Arguments.Item(0)
pw = WScript.Arguments.Item(1) 

cmd = "node server.js " + user + " " + pw

CreateObject("Wscript.Shell").Run cmd, 0