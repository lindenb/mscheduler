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
package com.github.lindenb.mscheduler;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

import org.slf4j.LoggerFactory;

import com.github.lindenb.mscheduler.io.IoUtils;
import com.github.lindenb.mscheduler.io.StreamBoozer;

public class SGEScheduler extends MScheduler {
	private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(SGEScheduler.class);

private SGEScheduler() {
	
}

private class SGEStatusChecker extends StatusChecker
	{
	SGEStatusChecker(final Task task) {
		super(task);
		}
	@Override
	public Integer call() throws Exception {
		final Pattern wsSplit = Pattern.compile("[ \t]+");
		/* first we try qstat, and then qacct */
		for(int side=0; side < 2;++side)
			{	
			BufferedReader in = null;
			try {
				final List<String> cmdargs= new ArrayList<>();
				cmdargs.add(side==0?"qstat":"qacct");
				cmdargs.add("-j");
				cmdargs.add(super.task.processId);
				LOG.info("checking `"+String.join(" ", cmdargs)+"`");
				final ProcessBuilder procbuilder= new ProcessBuilder(cmdargs);
				procbuilder.directory(getBaseDirectory());
				final Process proc = procbuilder.start();
				final StreamBoozer sb = new StreamBoozer(proc.getErrorStream(),System.err,"["+(side==0?"qstat":"qacct")+"]");
				sb.start();
				in =new BufferedReader(new InputStreamReader(proc.getInputStream()));
				String line;
				boolean found_in_qstat=false;
				boolean found_in_qacct=false;
				while((line=in.readLine())!=null)
					{
					LOG.info(line);
					final String tokens[]=wsSplit.split(line);
					if(tokens.length<2) continue;
					if(side==0) {
							if(tokens[0].equals("job_number:") && tokens[1].equals(task.processId)) {
							LOG.info("job "+super.task+" still running");
							found_in_qstat = true;
							task.targetStatus = TaskStatus.RUNNING;
							}
						}
					else
						{
						if(tokens[0].equals("exit_status") ) {
							found_in_qacct = true;
							if(tokens[1].equals("0")) {
								task.targetStatus = TaskStatus.COMPLETED;
							} else {
								LOG.info("job "+super.task+" FAILED");
								task.targetStatus = TaskStatus.ERROR;
								}
							}
						if(tokens[0].equals("failed") ) {
							found_in_qacct = true;
							if(tokens[1].equals("0")) {
								task.targetStatus = TaskStatus.COMPLETED;
							} else {
								LOG.info("job "+ super.task +" FAILED");
								task.targetStatus = TaskStatus.ERROR;
								}
							}
						}
					}
				in.close();
				in=null;
				
				int ret = proc.waitFor();
				if(ret!=0)
					{
					LOG.error("["+(side==0?"qstat":"qacct")+"] process failed");
					if(side==1) {
						return -1;
					}
					}
				if(found_in_qstat) return 0;
				if(found_in_qacct)  return 0;
				}
			catch (Exception e) {
				LOG.error("boum", e);
				return -1;				} 
			finally 
				{
				IoUtils.close(in);
				}
			}
		LOG.info("found neither in qstat or qacc");
		return -1;
		}
	}

@Override
protected StatusChecker createStatusChecker(final Task task) {
	return new SGEStatusChecker(task);
	}

@Override
protected void kill(final Task t) throws IOException {
	LOG.info("killing "+t);
	Runtime.getRuntime().exec("qdel "+t.processId);
	//lindenb has registered the job 393403 for deletion
}

@Override
protected int submitJob(final Task task) {
	LOG.info("Submitting "+task);
	PrintWriter pw = null;
	BufferedReader in=null;
	try {
		task.shellScriptFile = File.createTempFile("tmp.", ".bash",getWorkingDirectory());
		task.stdoutFile = File.createTempFile("tmp.", "."+task.nodeId+".stdout",getWorkingDirectory());
		task.stderrFile = File.createTempFile("tmp.", "."+task.nodeId+".stderr",getWorkingDirectory());
		pw = new PrintWriter(task.shellScriptFile);
		pw.println("#!/bin/bash");
		pw.println("#");
		pw.println("#$ -N n"+task.nodeId);
		pw.println("#$ -o "+task.stdoutFile.getPath());
		pw.println("#$ -e "+task.stderrFile.getPath());
		pw.println("#$ -cwd");
		pw.println("#$ -S /bin/bash");
		for(final String s:task.shellScriptLines) {
			if(!s.startsWith("#$")) continue;
			pw.println(s);
			}
		
		pw.println("cd '"+super.getBaseDirectory()+"' ;");
		
		for(final String s:task.shellScriptLines) {
			if(s.startsWith("#$")) continue;
			pw.println(s);
			}
		
		pw.flush();
		if(pw.checkError()) throw new IOException("Boum");
		pw.close();
		pw=null;
		
		makeExecutable(task.shellScriptFile);
		
		//Your job 393326 ("test.sh") has been submitted
		
		LOG.info("submitting "+task);
		final List<String> cmdargs= new ArrayList<>();
		cmdargs.add("qsub");
		cmdargs.add(task.shellScriptFile.getName());
		final ProcessBuilder procbuilder= new ProcessBuilder(cmdargs);
		procbuilder.directory(task.shellScriptFile.getParentFile());
		Process proc = procbuilder.start();
		StreamBoozer sb = new StreamBoozer(proc.getErrorStream(),System.err,"[qsub]");
		sb.start();
		in =new BufferedReader(new InputStreamReader(proc.getInputStream()));
		final String qsub_prefix="Your job ";
		String line;
		while((line=in.readLine())!=null)
			{
			if(!line.startsWith(qsub_prefix))
				{
				LOG.error("expected "+line+" to start with "+qsub_prefix);
				return -1;
				}
			final int par = line.indexOf("(");
			if(par<1)
				{
				LOG.error("cannot find parenthesis in "+line);
				return -1;
				}
			
			final long sgejobid = Long.parseLong(line.substring(qsub_prefix.length(),par).trim());
			if( sgejobid <= 0L )
				{
				LOG.error("Bad job id in  "+line);
				return -1;
				}
			task.processId=String.valueOf(sgejobid);
			}
		in.close();
		
		final int ret = proc.waitFor();
		if(ret!=0)
			{
			LOG.error("process failed : error "+ret);
			return -1;
			}
		
		if( task.processId == null )
			{
			LOG.error("Cannot get job id for "+task);
			return -1;
			}
		LOG.info("OK job ID =" + task.processId );
		
		task.startMilliSec = System.currentTimeMillis();
		task.targetStatus = TaskStatus.RUNNING;
		return 0;
	} catch (final Exception e) {
		LOG.error("boum", e);
		return -1;	
	} finally {
		IoUtils.close(pw);
	}
	
	}


public static void main(String[] args) {
	new SGEScheduler().instanceMainWithExit(args);
	}
}
