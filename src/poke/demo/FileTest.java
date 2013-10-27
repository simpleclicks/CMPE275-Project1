package poke.demo;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;

public class FileTest {

	
	public static void main(String args[]){
		
		File fileAway = new File("away");
		
		File abc = new File("away\\abc.txt");
		
	//	File awayTree =  new File(fileAway, "away.txt");
		
		try {
		
			
			
			System.out.println("away exists "+fileAway.exists());
			System.out.println("file abc exists "+abc.exists());
			
					System.out.println(FileUtils.directoryContains(fileAway, abc));
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
	}
	
}
