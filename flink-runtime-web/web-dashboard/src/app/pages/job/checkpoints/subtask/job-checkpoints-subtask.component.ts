/*
 *   Licensed to the Apache Software Foundation (ASF) under one
 *   or more contributor license agreements.  See the NOTICE file
 *   distributed with this work for additional information
 *   regarding copyright ownership.  The ASF licenses this file
 *   to you under the Apache License, Version 2.0 (the
 *   "License"); you may not use this file except in compliance
 *   with the License.  You may obtain a copy of the License at
 *       http://www.apache.org/licenses/LICENSE-2.0
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

import { Component, Input, OnInit } from '@angular/core';
import { first } from 'rxjs/operators';
import { deepFind } from 'flink-core';
import { JobService } from 'flink-services';

@Component({
  selector   : 'flink-job-checkpoints-subtask',
  templateUrl: './job-checkpoints-subtask.component.html',
  styleUrls  : [ './job-checkpoints-subtask.component.less' ]
})
export class JobCheckpointsSubtaskComponent implements OnInit {
  @Input() vertex;
  @Input() checkPoint;
  subTaskCheckPoint;
  listOfSubTaskCheckPoint = [];
  isLoading = true;
  sortName = null;
  sortValue = null;


  sort(sort: { key: string, value: string }) {
    this.sortName = sort.key;
    this.sortValue = sort.value;
    this.search();
  }

  search() {
    if (this.sortName) {
      this.listOfSubTaskCheckPoint = [ ...this.listOfSubTaskCheckPoint.sort(
        (pre, next) => {
          if (this.sortValue === 'ascend') {
            return (deepFind(pre, this.sortName) > deepFind(next, this.sortName) ? 1 : -1);
          } else {
            return (deepFind(next, this.sortName) > deepFind(pre, this.sortName) ? 1 : -1);
          }
        }) ];
    }
  }

  constructor(private jobService: JobService) {
  }

  ngOnInit() {
    this.jobService.jobDetail$.pipe(
      first()
    ).subscribe(() => {
      this.jobService.loadCheckpointSubtaskDetails(this.jobService.jobDetail.jid, this.checkPoint.id, this.vertex.id).subscribe(data => {
        this.subTaskCheckPoint = data;
        this.listOfSubTaskCheckPoint = (data && data[ 'subtasks' ]) || [];
        this.isLoading = false;
      }, () => {
        this.isLoading = false;
      });
    });
  }

}
