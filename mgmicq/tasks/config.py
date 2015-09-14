
amplicon_workflow_config = {"mgmic.oscer.ou.edu":
						{"docker_opts":["-v /data:/data -v /opt/local/scripts:/opt/local/scripts",
										"-i -t -v /opt:/opt -v /data:/data"],
                         "docker_cmd":[ "/opt/local/scripts/bin/Illumina_MySeq_16SAmplicon_analysis_part1.pl %s %s %s",
                                        "/opt/local/scripts/bin/Illumina_MySeq_16SAmplicon_analysis_part2.pl %s %s %s"]
                        },
                    "10.196.99.178":
                    	{"docker_opts":["-v /Users/mstacy/data_munge/risser_crazy/data:/data -v /Users/mstacy/data_munge/risser_crazy/data/local/scripts:/opt/local/scripts",
                    					"-i -t -v /Users/mstacy/data_munge/risser_crazy/data:/data -v /Users/mstacy/data_munge/risser_crazy/data/local/scripts:/opt/local/scripts"],
                         "docker_cmd":[  "/opt/local/scripts/bin/Illumina_MySeq_16SAmplicon_analysis_part1.pl %s %s %s",
                                         "/opt/local/scripts/bin/Illumina_MySeq_16SAmplicon_analysis_part2.pl %s %s %s"]
                        },
             		}
