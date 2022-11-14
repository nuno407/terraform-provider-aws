import { Component, Input, OnInit } from '@angular/core';
import { Label } from 'src/app/models/label';
import { LabelingService } from 'src/app/core/services/labeling.service';

@Component({
  selector: 'app-sidebar',
  templateUrl: './sidebar.component.html',
  styleUrls: ['./sidebar.component.scss'],
})
export class SidebarComponent implements OnInit {
  /**Local variables */
  currentFrame: number;
  selectedLabelIndex: number;
  selectedFrame: string;

  labels: Label[];

  @Input() fps: number;
  @Input()
  set frame(value: number) {
    this.currentFrame = value;
    if (this.selectedLabelIndex != undefined && this.selectedFrame != undefined) {
      let label: Label = this.labels[this.selectedLabelIndex];
      label[this.selectedFrame].frame = value;
      label[this.selectedFrame].seconds = value / this.fps;
      this.labelingService.updateLabel(this.selectedLabelIndex, label);
    }
  }

  constructor(private labelingService: LabelingService) {}

  /**Aplication state */
  ngOnInit() {
    this.labelingService.getLabels().subscribe((labels) => {
      this.labels = labels;
    });
    this.labelingService.getSelectedLabelIndex().subscribe((index) => {
      if (index == -1) {
        this.selectedLabelIndex = null;
        this.selectedFrame = null;
      } else {
        this.selectedLabelIndex = index;
      }
    });
  }

  addLabel() {
    this.labelingService.addLabel(
      new Label(
        {
          frame: this.currentFrame,
          seconds: this.currentFrame / this.fps,
        },
        {
          frame: this.currentFrame + this.fps * 2,
          seconds: this.currentFrame / this.fps + 2,
        }
      )
    );
  }

  onLabelDelete(index: number) {
    this.labelingService.deleteLabel(index);
  }

  onLabelEdit(index: number, edit: boolean) {
    if (edit) {
      this.labelingService.setSelectedLabelIndex(index);
    } else {
      this.labelingService.setSelectedLabelIndex(-1);
    }
  }

  onFrameSelect(value: string) {
    this.selectedFrame = value;
  }

  onVisibilityToggle(index: number, value: boolean) {
    let label = this.labels[index];
    label.visibility = value;
    this.labelingService.updateLabel(index, label);
  }
}
