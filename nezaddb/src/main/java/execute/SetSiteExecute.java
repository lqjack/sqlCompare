package execute;

import java.util.List;

import gdd.GDD;
import gdd.SiteMeta;
import parserResult.ParseResult;
import parserResult.SetSiteResult;

public class SetSiteExecute extends ExecuteSQL{
	private GDD gdd;
	
	public SetSiteExecute(){
		gdd = GDD.getInstance();
	}
	
	@Override
	public void execute(ParseResult result) {
		SetSiteResult setSiteResult = (SetSiteResult)result;
		List<SetSiteResult.SiteIpMap> siteIpMaps = setSiteResult.getSiteIpMap();	
		int i;
		for(i = 0 ; i < siteIpMaps.size() ; i++){
			SetSiteResult.SiteIpMap siteIpMap = siteIpMaps.get(i);
			SiteMeta site = new SiteMeta(siteIpMap.siteName,siteIpMap.siteIP,siteIpMap.sitePort);
			(gdd.getSiteInfo()).add(site);
			gdd.addSiteNum();
			
		}
	}

}
