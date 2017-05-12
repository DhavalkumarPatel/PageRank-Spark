package mr.hw4.java.datatypes;

import java.util.Set;

/**
 * This class is used to store Node Id and Adjacency List of that
 * Node.
 * @author dspatel
 */
public class Node
{	
	private String nodeId;
	private Set<String> adjacencyList;
    
    public Node(String nodeId, Set<String> adjacencyList)
    {
    	this.nodeId = nodeId;
    	this.adjacencyList = adjacencyList;
    }
    
    public String getNodeId() 
    {
		return nodeId;
	}
    
    public String[] getAdjacencyList() 
    {
		return adjacencyList.toArray(new String[adjacencyList.size()]);
	}
}