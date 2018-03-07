package diskmgr;

public class PCounter
{
	public static int rcounter;
	public static int wcounter;

	/*
	 * Initially, there are zero reads and writes 
	 */
	public static void initialize()
	{
		rcounter = 0;
		wcounter = 0;
	}

	/*
	 * Increment by 1 upon a disk read request
	 */
	public static void readIncrement()
	{
		rcounter++;
	}

	/*
	 * Increment by 1 upon a disk write request
	 */
	public static void writeIncrement()
	{
		wcounter++;
	}
}