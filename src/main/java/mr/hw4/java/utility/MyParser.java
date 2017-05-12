package mr.hw4.java.utility;

import java.net.URLDecoder;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;

import mr.hw4.java.datatypes.Node;

/**
 * MyParser uses Jsoup API to parse the html document and retrieve adjacency List.
 */
public class MyParser 
{
	private static Pattern namePattern;
	private static Pattern linkPattern;
	
	static 
	{
		// Keep only html pages not containing tilde (~).
		namePattern = Pattern.compile("^([^~]+)$");
		// Keep only html filenames ending relative paths and not containing tilde (~).
		linkPattern = Pattern.compile("^\\..*/([^~]+)\\.html$");
	}
	
	/**
	 * It parses the line and returns nodeId and adjacencyList as a Node object.
	 * It returns null if input does not satisfy all requirements as per HW description.
	 * @param line
	 * @return
	 */
	public static Node parse(String line)
	{
		try
		{
			int delimLoc = line.indexOf(':');
			
			// Parse nodeId
			String nodeId = line.substring(0, delimLoc);
			Matcher matcher = namePattern.matcher(nodeId);
			if (!matcher.find()) 
			{
				// Skip this html file, nodeId contains (~).
				return null;
			}
			
			// Parse adjacencyList
			String html = line.substring(delimLoc + 1);
			Set<String> adjacencyList = new HashSet<String>();
			
			// parse html string to jsoup document
			Document doc = Jsoup.parse(html);
			
			// find links from the document and update adjacency list
			for(Element element : doc.getElementsByAttributeValue("id", "bodyContent"))
			{
				if(null != element && null != element.tagName() && element.tagName().equalsIgnoreCase("div"))
				{
					for(Element linkElem : element.select("a[href]"))
					{
						String link = linkElem.attr("href");
						if (null != link) 
						{
							link = URLDecoder.decode(link, "UTF-8");
							Matcher linkMatcher = linkPattern.matcher(link);
							if (linkMatcher.find()) 
							{
								adjacencyList.add(linkMatcher.group(1));
							}
						}
					}
				}
			}
			
			// remove self links
			adjacencyList.remove(nodeId);
			
			return new Node(nodeId, adjacencyList);
		}
		catch(Exception e)
		{
			System.err.println(e.getMessage());
		}
		return null;
	}
}
