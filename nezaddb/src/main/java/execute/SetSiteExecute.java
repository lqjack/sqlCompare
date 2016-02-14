package execute;

import java.util.List;

import gdd.GDD;
import gdd.SiteInfo;
import parserResult.ParseResult;
import parserResult.SetSiteResult;

public class SetSiteExecute extends ExecuteSQL{
	private GDD gdd;
	
	public SetSiteExecute(){
		gdd = GDD.getInstance();
	}
	
	@Override
	public void execute(ParseResult result) {
		// TODO Auto-generated method stub
		SetSiteResult setSiteResult = (SetSiteResult)result;
		List<SetSiteResult.SiteIpMap> siteIpMaps = setSiteResult.getSiteIpMap();	
		int i;
		for(i = 0 ; i < siteIpMaps.size() ; i++){
			SetSiteResult.SiteIpMap siteIpMap = siteIpMaps.get(i);
			SiteInfo site = new SiteInfo(siteIpMap.siteName,siteIpMap.siteIP,siteIpMap.sitePort);
			(gdd.getSiteInfo()).add(site);
			gdd.addSiteNum();
			
		}
	}

}
