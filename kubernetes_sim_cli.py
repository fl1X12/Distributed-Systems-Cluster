import argparse
import requests
import sys
import json
import os
import pickle
from tabulate import tabulate

API_SERVER_URL = os.environ.get("API_SERVER_URL", "http://localhost:5000")
NODE_REGISTRY_FILE = "node_registry.pkl"

# Dictionary to keep track of running nodes
active_nodes = {}

# Load existing nodes from file if it exists
def load_nodes():
    global active_nodes
    try:
        if os.path.exists(NODE_REGISTRY_FILE):
            with open(NODE_REGISTRY_FILE, 'rb') as f:
                active_nodes = pickle.load(f)
            print(f"Loaded {len(active_nodes)} existing nodes from registry")
    except Exception as e:
        print(f"Warning: Could not load node registry: {e}")

# Save nodes to file
def save_nodes():
    try:
        with open(NODE_REGISTRY_FILE, 'wb') as f:
            pickle.dump(active_nodes, f)
    except Exception as e:
        print(f"Warning: Could not save node registry: {e}")

def add_node(args):
    """Add a new node to the cluster"""
    try:
        cpu_cores = int(args.cpu_cores)
        if cpu_cores <= 0:
            print("Error: CPU cores must be a positive number")
            return 1
        
        # This will call out to the node_container.py script to create a container
        from node_container import NodeContainer
        
        # Create a new node container
        node = NodeContainer(cpu_cores, API_SERVER_URL)
        success = node.start()
        
        if success:
            # Store the node reference so it persists
            active_nodes[node.node_id] = node
            save_nodes()
            
            print(f"✅ Node added successfully with ID: {node.node_id}")
            print(f"   CPU Cores: {cpu_cores}")
            return 0
        else:
            print("❌ Failed to add node")
            return 1
            
    except ValueError:
        print("Error: CPU cores must be a number")
        return 1
    except Exception as e:
        print(f"❌ Error: {str(e)}")
        return 1

def list_nodes(args):
    """List all nodes in the cluster"""
    try:
        response = requests.get(f"{API_SERVER_URL}/nodes")
        
        if response.status_code == 200:
            data = response.json()
            nodes = data["nodes"]
            
            if not nodes:
                print("No nodes found in the cluster")
                return 0
            
            # Prepare data for tabular display
            table_data = []
            for node in nodes:
                status_display = node["status"]
                if status_display == "Ready":
                    status_display = "✅ Ready"
                elif status_display == "NotReady":
                    status_display = "⚠️ NotReady"
                else:
                    status_display = "❌ Failed"
                
                # Check if this node is in our active_nodes dict
                is_active = node["node_id"] in active_nodes
                active_status = "Running" if is_active else "Unknown"
                
                table_data.append([
                    node["node_id"][:8] + "...",  # Shortened ID
                    status_display,
                    node["cpu_cores"],
                    node["available_cores"],
                    len(node["pods"]),
                    active_status
                ])
            
            headers = ["Node ID", "Status", "Total CPU", "Available CPU", "Pod Count", "Process Status"]
            print(tabulate(table_data, headers=headers, tablefmt="grid"))
            print(f"\nTotal Nodes: {len(nodes)}")
            return 0
        else:
            print(f"❌ Error: Failed to list nodes. Status code: {response.status_code}")
            print(response.text)
            return 1
    
    except Exception as e:
        print(f"❌ Error: {str(e)}")
        return 1

def stop_node(args):
    """Stop a specific node by ID"""
    node_id = args.node_id
    
    if node_id in active_nodes:
        try:
            print(f"Stopping node {node_id}...")
            active_nodes[node_id].stop()
            del active_nodes[node_id]
            save_nodes()
            print(f"✅ Node {node_id} stopped successfully")
            return 0
        except Exception as e:
            print(f"❌ Error stopping node: {e}")
            return 1
    else:
        # Try to look up the node in the API server
        try:
            response = requests.get(f"{API_SERVER_URL}/nodes")
            if response.status_code == 200:
                nodes = response.json()["nodes"]
                for node in nodes:
                    if node["node_id"] == node_id:
                        print(f"Node found in API server but not in local registry.")
                        print(f"Cannot stop this node as it was likely created in another session.")
                        return 1
            
            print(f"❌ Node with ID {node_id} not found")
            return 1
        except Exception as e:
            print(f"❌ Error: {e}")
            return 1

def stop_all_nodes(args):
    """Stop all running nodes"""
    if not active_nodes:
        print("No active nodes to stop")
        return 0
    
    print(f"Stopping {len(active_nodes)} nodes...")
    errors = 0
    
    for node_id, node in list(active_nodes.items()):
        try:
            print(f"Stopping node {node_id}...")
            node.stop()
            del active_nodes[node_id]
        except Exception as e:
            print(f"❌ Error stopping node {node_id}: {e}")
            errors += 1
    
    save_nodes()
    
    if errors:
        print(f"✅ Stopped {len(active_nodes) - errors} nodes with {errors} errors")
        return 1
    else:
        print(f"✅ All nodes stopped successfully")
        return 0

def main():
    # Load existing nodes when the program starts
    load_nodes()
    
    parser = argparse.ArgumentParser(description="Kubernetes-like Cluster Simulation CLI")
    subparsers = parser.add_subparsers(dest="command", help="Command to execute")
    
    # Add node command
    add_node_parser = subparsers.add_parser("add-node", help="Add a new node to the cluster")
    add_node_parser.add_argument("cpu_cores", help="Number of CPU cores for the node")
    
    # List nodes command
    list_nodes_parser = subparsers.add_parser("list-nodes", help="List all nodes in the cluster")
    
    # Stop node command
    stop_node_parser = subparsers.add_parser("stop-node", help="Stop a specific node")
    stop_node_parser.add_argument("node_id", help="ID of the node to stop")
    
    # Stop all nodes command
    stop_all_parser = subparsers.add_parser("stop-all", help="Stop all running nodes")
    
    # Parse arguments
    args = parser.parse_args()
    
    if args.command == "add-node":
        return add_node(args)
    elif args.command == "list-nodes":
        return list_nodes(args)
    elif args.command == "stop-node":
        return stop_node(args)
    elif args.command == "stop-all":
        return stop_all_nodes(args)
    else:
        parser.print_help()
        return 0

if __name__ == "__main__":
    sys.exit(main())