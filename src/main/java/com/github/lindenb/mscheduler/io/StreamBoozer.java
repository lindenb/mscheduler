/*
The MIT License (MIT)

Copyright (c) 2016 Pierre Lindenbaum

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.

*/
package com.github.lindenb.mscheduler.io;


import java.io.InputStream;
import java.io.PrintStream;

import org.slf4j.LoggerFactory;


public class StreamBoozer extends Thread
	{
	private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(StreamBoozer.class);

    private final InputStream in;
    private final PrintStream pw;
	private final String prefix;

	public StreamBoozer(final InputStream in,final PrintStream pw,final String prefix)
		{
        this.in = in;
        this.pw = pw;
        this.prefix=prefix;
		}

    @Override
    public void run()
    	{
    	boolean begin=true;
    	try {
    		int c;
    		while((c=in.read())!=-1)
    			{
    			if(begin) pw.print(prefix);
    			pw.write((char)c);
    			begin=(c=='\n');
    			}
    	 	}
    	catch(Exception err)
    		{
    		LOG.error("StreamBoozer error", err);
    		}
    	finally
    		{
    		IoUtils.close(in);
    		}
    	}
	}