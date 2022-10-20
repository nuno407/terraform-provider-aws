import { Component, OnInit } from '@angular/core';
import { Activity } from 'src/app/models/activity';
import { Label } from 'src/app/models/label';
import { LabelingService } from 'src/app/core/services/labeling.service';

@Component({
  selector: 'app-activities',
  templateUrl: './activities.component.html',
  styleUrls: ['./activities.component.scss'],
})
export class ActivitiesComponent implements OnInit {
  /**Local variables */
  activities: Activity = new Activity();
  labels: Label[];
  selectedLabelIndex: number;

  constructor(private labelingService: LabelingService) {}

  /**Aplication state */
  ngOnInit(): void {
    this.labelingService.getLabels().subscribe((labels) => {
      this.labels = labels;
    });

    this.labelingService.getSelectedLabelIndex().subscribe((index) => {
      if (index == -1) {
        this.selectedLabelIndex = null;
        this.activities = new Activity();
      } else {
        this.selectedLabelIndex = index;
        this.activities = this.labels[this.selectedLabelIndex].activities;
      }
    });
  }

  radioChange(activity, index) {
    const label = this.labels[this.selectedLabelIndex];
    label.activities[activity].selected = index;
    this.labelingService.updateLabel(this.selectedLabelIndex, label);
  }

  toggleChange(activity, value) {
    const label = this.labels[this.selectedLabelIndex];
    if (activity === 'physicalAggression.weapon') {
      label.activities.physicalAggression.weapon.value = value.checked;
    } else {
      label.activities[activity] = value.checked;
    }

    this.labelingService.updateLabel(this.selectedLabelIndex, label);
  }

  changeOccupants(number) {
    const label = this.labels[this.selectedLabelIndex];
    label.activities.occupants = Math.max(number, 0);

    this.labelingService.updateLabel(this.selectedLabelIndex, label);
  }
}
