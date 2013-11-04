/*
 * copyright 2012, gash
 * 
 * Gash licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package poke.demo;

import java.io.File;
import java.io.BufferedReader;
import java.io.InputStreamReader;


import poke.client.ClientConnection;
import poke.client.ClientListener;
import poke.client.ClientPrintListener;

public class Jab {

	private String tag;
	private int count;
	private String sep = File.separator;


	public Jab(String tag) {
	//	this.tag = tag;
	}

	public void run() {
		ClientConnection cc = ClientConnection.initConnection("192.168.0.181", 5575);
		System.out.println("Adding listener");
		ClientListener listener = new ClientPrintListener("jab demo");
		cc.addListener(listener);


		String namespace = null;
		String docName = null;
		
		try {
			while (true) {
				
				BufferedReader buffer = new BufferedReader(new InputStreamReader(System.in));

				System.out.println("Select one function to be executed" +
						" 1.Add namespace "+"\n"+" 2.Delete Namespace "+"\n"+"3.List all files in the namespace"+"\n"+" 4.Add Document "+"\n"+"5.Delete Document"+"\n"+" 6.Find Document "+"\n"+"7.Exit");
				int choice = Integer.parseInt (buffer.readLine());
				
				switch (choice) {
				case 1:
					System.out.println("Enter the namespace to be added ");
					namespace = buffer.readLine();
					cc.namespaceAdd(namespace);
					break;
				case 2:
					System.out.println("Enter the namespace to be deleted ");
					namespace = buffer.readLine();
					cc.namespaceRemove(namespace);
					break;
				case 3:
					System.out.println("Enter the namespace to be to list all the file names ");
					namespace = buffer.readLine();
					cc.namespaceList(namespace);
					break;
				case 4:
					System.out.println("Enter the namespace ");
					namespace = buffer.readLine();
					System.out.println("Enter the document name ");
					docName = buffer.readLine();
					cc.docAddReq(namespace,docName);
					break;
				case 5:
					System.out.println("Enter the namespace ");
					namespace = buffer.readLine();
					System.out.println("Enter the document name ");
					docName = buffer.readLine();
					cc.docRemove(namespace, docName);
					break;
				case 6:
					System.out.println("Enter the namespace ");
					namespace = buffer.readLine();
					System.out.println("Enter the document name ");
					docName = buffer.readLine();
					cc.docFind(namespace,docName);
					break;
				case 7:
					System.exit(0);
					break;
				default:
					break;
				}
			}
		} catch (Exception e) {
			// TODO: handle exception
		}
	
		}
	

	public static void main(String[] args) {
		try {
			Jab jab = new Jab("jab");
			jab.run();

			// we are running asynchronously
			System.out.println("\nExiting in 5 seconds");
			//Thread.sleep(5000);
			//System.exit(0);

		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
