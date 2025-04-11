#!/bin/sh

# Avoid errors if Xvfb is already running
rm -f /tmp/.X99-lock /tmp/.X11-unix/X99

echo "Starting X virtual framebuffer (Xvfb)..."

# Defines var DISPLAY to be detectable by Chrome as a virtual display
export DISPLAY=:99

# Starts Xvfb at display :99
Xvfb -ac :99 -screen 0 1920x1080x24 &


# VNC Debugging
sleep 2

echo ">> Starting window manager (fluxbox)..."
fluxbox &

echo ">> Starting VNC server..."
x11vnc -display :99 -forever -passwd secret -shared -rfbport 5900 &

sleep 2





# Executes the command passed to container
exec "$@"
