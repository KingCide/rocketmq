#!/bin/bash
set -e # Exit immediately if a command exits with a non-zero status.

# --- 配置 ---
TARGET_IP="127.0.0.1"
# !!! crucial: Replace 10911 with the actual port found in step 1 !!!
TARGET_PORT="10911"
TARGET_PORT2="9876"
TARGET_PORT="8081"
BLOCK_DURATION=20 # seconds
# Optional: Specify consumer source port if needed, but usually not required
# SOURCE_PORT=""
# --- 配置结束 ---

# Construct the iptables rule
# Blocks outgoing TCP packets TO the target IP and port
RULE_SPEC="-p tcp -d ${TARGET_IP} --dport ${TARGET_PORT} -j DROP"
#RULE_SPEC2="-p tcp -d ${TARGET_IP} --dport ${TARGET_PORT2} -j DROP"
# Uncomment the next line and comment the previous one if you need to specify source port
# RULE_SPEC="-p tcp -s <Consumer_IP_if_not_localhost> --sport ${SOURCE_PORT} -d ${TARGET_IP} --dport ${TARGET_PORT} -j DROP"

# Add the blocking rule using -I (Insert) to put it at the beginning of the OUTPUT chain
echo "Adding iptables rule: sudo iptables -I OUTPUT 1 ${RULE_SPEC}"
sudo iptables -I OUTPUT 1 ${RULE_SPEC}
#echo "Adding iptables rule: sudo iptables -I OUTPUT 1 ${RULE_SPEC2}"
#sudo iptables -I OUTPUT 1 ${RULE_SPEC2}

# Start a background process to remove the rule after the delay
(
  sleep ${BLOCK_DURATION}
  echo "Removing iptables rule after ${BLOCK_DURATION} seconds: sudo iptables -D OUTPUT ${RULE_SPEC}"
  # Use the same rule specification to delete it
  sudo iptables -D OUTPUT ${RULE_SPEC} || echo "Rule might have already been removed or changed."
  #echo "Removing iptables rule after ${BLOCK_DURATION} seconds: sudo iptables -D OUTPUT ${RULE_SPEC2}"
  #sudo iptables -D OUTPUT ${RULE_SPEC2} || echo "Rule might have already been removed or changed."
  echo "Block removed."
) & disown # disown prevents the background job from being killed

echo "Blocking ACKs to ${TARGET_IP}:${TARGET_PORT} using iptables for ${BLOCK_DURATION} seconds... (Rule removal runs in background)"

# Exit the main script successfully. The cleanup is handled by the background process.
exit 0
