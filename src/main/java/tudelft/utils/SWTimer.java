/****************************************/
//	Class Name:	SWTimer	
//	Author:		Hamid Mushtaq  		
//	Company:	TU Delft	 	
/****************************************/
package tudelft.utils;

public class SWTimer 
{
	private long st;
	private long et;
	private long elapsed = 0;

	public void start() 
	{ 
		st = System.nanoTime();
	}
	
	public void stop() 
	{ 
		et = System.nanoTime();
		elapsed += (et - st);
	}
	
	public void reset()				{elapsed = 0;}
	public long getMicroSecs() 		{return elapsed / (long)1e3;}
	public long getMilliSecs() 		{return elapsed / (long)1e6;}
	public long getSecs() 			{return elapsed / (long)1e9;}
	public float getMilliSecsF()	{return elapsed / (float)1e6;}
	public float getSecsF() 		{return elapsed / (float)1e9;}
}
