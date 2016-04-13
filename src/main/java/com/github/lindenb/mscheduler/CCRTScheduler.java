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
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.regex.Pattern;

import org.slf4j.LoggerFactory;

import com.github.lindenb.mscheduler.io.IoUtils;
import com.github.lindenb.mscheduler.io.StreamBoozer;


public class CCRTScheduler extends MScheduler {
	private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(CCRTScheduler.class);

	private enum JobStatus
		{
		CONFIGURING,
		CANCELLED,
		COMPLETED,
		FAILED,
		RUNNING,
		NODE_FAIL,
		PENDING,
		PREEMPTED,
		SUSPENDED,
		TIMEOUT
		}
	
	
	private class SacctCall extends StatusChecker
		{
		SacctCall(final Task task)
			{
			super(task);
			}
		
		@Override
		public Integer call() throws Exception
			{
			StreamBoozer sb = null;
			BufferedReader in=null;
			try
				{
				/* we call sacct to get the job */
				final Pattern delimPipes = Pattern.compile("[\\|]");
				final List<String> cmdargs= new ArrayList<>();
				cmdargs.add("sacct");
				cmdargs.add("-p");
				cmdargs.add("-j");
				cmdargs.add(super.task.processId);
				final ProcessBuilder procbuilder= new ProcessBuilder(cmdargs);
				procbuilder.directory(getWorkingDirectory());
				final Process proc = procbuilder.start();
				sb = new StreamBoozer(proc.getErrorStream(),System.err,"[sacct]");
				sb.start();
				in =new BufferedReader(new InputStreamReader(proc.getInputStream()));
				JobStatus newStatus = JobStatus.RUNNING;
				String line;
				while((line=in.readLine())!=null )
					{
					//ignore header
					if(line.trim().isEmpty()) continue;
					if(line.startsWith("JobID|")) continue;
					String tokens[]=delimPipes.split(line);
					if(tokens.length<7)
						{
						LOG.error("expected 7 tokens in "+line);
						return -1;
						}
					if(!super.task.processId.equals(tokens[0]))
						{
						continue;
						}
					if( tokens[5].startsWith("CANCELLED "))
						{
						LOG.debug(tokens[5]+" for JOB ID. "+super.task.processId);
						tokens[5] = "CANCELLED";
						}
					try
						{
						newStatus = JobStatus.valueOf(tokens[5]);
						
						switch(newStatus)
							{
							case PENDING:
							case SUSPENDED:
							case CONFIGURING: 
							case RUNNING:
								{
								break;
								}
							case TIMEOUT: 
							case CANCELLED:
							case FAILED:
							case NODE_FAIL:
								{
								LOG.info("job failed");
								super.task.targetStatus = TaskStatus.ERROR;
								break;
								}
							case COMPLETED: 
								{
								LOG.info("job completed");
								super.task.targetStatus = TaskStatus.COMPLETED;
								}
							default: throw new IllegalStateException(task+" "+newStatus);
							}
						}
					catch(Exception err2)
						{
						LOG.error("boum",err2);
						LOG.error("Bad JobStatus: \""+tokens[5]+"\"  for JOBID."+ super.task);
						return -1;
						}
					break;
					}
				IoUtils.close(in);
				int return_status = proc.waitFor();
				if(return_status!=0)
					{
					LOG.error("process sacct failed");
					return return_status;
					}
				return 0;
				}
			catch (Exception e)
				{
				LOG.error("failure", e);
				return -1;
				}
			finally
				{
				IoUtils.close(in);
				IoUtils.close(sb);
				}
			}
		}

	@Override
	protected StatusChecker createStatusChecker(final Task task) {
		return new SacctCall(task);
		}
	
	@Override
	protected void kill(final Task t) throws IOException {
		Runtime.getRuntime().exec("ccc_mdel "+t.processId);		
	}

	@Override
	protected int submitJob(final Task t) {
	BufferedReader in = null;
	try {
		t.targetStatus  =  TaskStatus.RUNNING;
		final List<String> lines=new ArrayList<>();
		boolean got_ccmsub_c = false;
		boolean got_ccmsub_q = false;
		boolean got_ccmsub_o = false;
		boolean got_ccmsub_e = false;
		boolean got_ccmsub_r = false;
		for(final String line:t.shellScriptLines )
			{
			if(line.startsWith("#MSUB"))
				{
				if(line.contains("-c")) got_ccmsub_c = true;
				else if(line.contains("-q")) got_ccmsub_q = true;
				else if(line.contains("-o")) got_ccmsub_q = true;
				else if(line.contains("-e")) got_ccmsub_q = true;
				else if(line.contains("-r")) got_ccmsub_r = true;
				else if(line.contains("-T"))
					{
					
					}
				}
			lines.add(line);
			}
		
		int header_index=0;
		
		
		t.shellScriptFile = File.createTempFile("_make", ".bash",getWorkingDirectory());
		if( !got_ccmsub_e)
			{
			t.stderrFile = new File(
					t.shellScriptFile.getParentFile(),
					t.shellScriptFile.getName()+".stderr"
					);
			}
		
		if( !got_ccmsub_o)
			{
			t.stdoutFile = new File(
					t.shellScriptFile.getParentFile(),
					t.shellScriptFile.getName()+".stdout"
					);
			}
		
		LOG.info(t.shellScriptFile.getPath());
		PrintWriter fw= new PrintWriter(t.shellScriptFile);
		fw.println("#!/bin/bash");
		while(header_index< lines.size() && lines.get(header_index).startsWith("#"))
			{
			fw.println(lines.get(header_index));
			header_index++;
			}
		if(!got_ccmsub_c)
			{
			int n_cores = this.countProcessors(t.shellScriptLines);
			fw.println("#MSUB -c "+n_cores);
			}
		if(!got_ccmsub_q)
			{
			fw.println("#MSUB -q large");
			}
		if(!got_ccmsub_e && t.stderrFile!=null)
			{
			fw.println("#MSUB -e "+t.stderrFile.getPath());
			}
		if(!got_ccmsub_o && t.stdoutFile!=null)
			{
			fw.println("#MSUB -o "+t.stdoutFile.getPath());
			}
		if(!got_ccmsub_r)
			{
			fw.println("#MSUB -r "+t.md5());
			}
		fw.println("# Date "+new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date()));
		fw.println("set -euf");
		fw.println("cd '"+getBaseDirectory().getAbsolutePath()+"'");
		
			
		while(header_index< lines.size() )
			{
			fw.println(lines.get(header_index));
			header_index++;
			}
		fw.println("\nsleep 5");
		fw.flush();
		fw.close();
		//make executable
		makeExecutable(t.shellScriptFile);
		
		
		
		/** submit job */
		LOG.info("submitting "+t);
		final List<String> cmdargs= new ArrayList<>();
		cmdargs.add("ccc_msub");
		cmdargs.add("-q");
		cmdargs.add("large");
		cmdargs.add(t.shellScriptFile.getName());
		final ProcessBuilder procbuilder= new ProcessBuilder(cmdargs);
		procbuilder.directory(t.shellScriptFile.getParentFile());
		final Process proc = procbuilder.start();
		final StreamBoozer sb = new StreamBoozer(proc.getErrorStream(),System.err,"[ccc_msub]");
		sb.start();
		String line;
		in =new BufferedReader(new InputStreamReader(proc.getInputStream()));
		final String cc_msub_prefix="Submitted Batch Session ";
		while((line=in.readLine())!=null)
			{
			if(!line.startsWith(cc_msub_prefix))
				{
				LOG.error("expected "+line+" to start with "+cc_msub_prefix);
				return -1;
				}
			t.processId = line.substring(cc_msub_prefix.length()).trim();
			if( Long.parseLong(t.processId) <= 0L )
				{
				LOG.error("Bad job id in  "+line);
				return -1;
				}
			}
		in.close();
		
		int ret = proc.waitFor();
		if(ret!=0)
			{
			LOG.error("process failed");
			return -1;
			}
		
		if(t.processId == null)
			{
			LOG.error("Cannot get job id for "+t);
			return -1;
			}
		LOG.info("OK job ID =" + t.processId );
		t.startMilliSec = System.currentTimeMillis();
		t.targetStatus = TaskStatus.RUNNING;
	return 0;
} catch (Exception e) {
	return -1;
} finally {
	IoUtils.close(in);
}
	}

	
	public static void main(String[] args) {
		new CCRTScheduler().instanceMainWithExit(args);
		}

}
