import { Component, EventEmitter, Input, Output } from '@angular/core';
import { Label } from 'src/app/models/label';

@Component({
  selector: 'app-label-entry',
  templateUrl: './label-entry.component.html',
  styleUrls: ['./label-entry.component.scss'],
})
export class LabelEntryComponent {
  /**Local variables */
  dateStartString: string;
  selectedFrame: string;
  visibility: boolean = true;

  @Input() fps: number;
  @Input() data: Label;
  @Input() inEditMode: boolean;

  @Output() edit = new EventEmitter();
  @Output() selectFrame = new EventEmitter<string>();
  @Output() delete = new EventEmitter();
  @Output() toggleVisibility = new EventEmitter();

  constructor() {}

  _editLabel() {
    this.selectedFrame = undefined;
    this.edit.emit(!this.inEditMode);
  }

  _selectFrame(value: string) {
    if (!this.inEditMode) return;

    if (this.selectedFrame == value) {
      this.selectedFrame = undefined;
    } else {
      this.selectedFrame = value;
    }
    this.selectFrame.emit(this.selectedFrame);
  }

  _delete() {
    this.delete.emit();
  }

  _toggleVisibility() {
    this.visibility = !this.visibility;
    this.toggleVisibility.emit(this.visibility);
  }
}
