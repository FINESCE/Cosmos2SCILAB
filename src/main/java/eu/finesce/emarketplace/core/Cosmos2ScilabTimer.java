/*
 * (C) Copyright 2014 FINESCE-WP4.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package eu.finesce.emarketplace.core;


import java.util.Timer;


public class Cosmos2ScilabTimer {

	public static void main(String[] args) throws InterruptedException {
		
		// Execute Cosmos2ORtlab class every 15 minutes  
		Cosmos2Scilab cosmos2rtlab = new Cosmos2Scilab("cosmos2scilab.properties");
		Timer timer = new Timer();
		timer.schedule(cosmos2rtlab, 0, 900000);
		}
}
