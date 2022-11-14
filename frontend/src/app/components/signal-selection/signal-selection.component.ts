import { Component, EventEmitter, Input, Output, SimpleChanges } from '@angular/core';
import { MatCheckboxChange } from '@angular/material/checkbox';
import { SignalGroup } from '../../models/parsedSignals';

@Component({
  selector: 'app-signal-selection',
  templateUrl: './signal-selection.component.html',
  styleUrls: ['./signal-selection.component.scss'],
})
export class SignalSelectionComponent {
  defaultVisibleSignals = [
    'interior_camera_health_response_cvb',
    'interior_camera_health_response_cve',
    'CameraViewBlocked',
    'CameraVerticalShifted',
    'Snapshots',
  ];

  @Input()
  signalsToSelect: SignalGroup;

  @Output()
  selectedSignals: EventEmitter<SignalGroup> = new EventEmitter<SignalGroup>();

  ngOnChanges(changes: SimpleChanges) {
    if (changes.signalsToSelect?.currentValue) {
      this.selectDefault(this.signalsToSelect);
      this.updateSelectedSignals();
    }
  }

  selectDefault(group: SignalGroup) {
    for (let grp of group.groups) {
      this.selectDefault(grp);
    }
    for (let sig of group.signals) {
      sig.enabled = this.defaultVisibleSignals.includes(sig.name);
    }
  }

  selectAll(group: SignalGroup, value: MatCheckboxChange) {
    for (let grp of group.groups) {
      this.selectAll(grp, value);
    }
    for (let sig of group.signals) {
      sig.enabled = value.checked;
    }
  }

  someSelected(group: SignalGroup): boolean {
    return this.getSelected(group) == Selection.Some;
  }

  allSelected(group: SignalGroup): boolean {
    return this.getSelected(group) == Selection.All;
  }

  private getSelected(group: SignalGroup): Selection {
    let oneOrMoreSelected = false;
    let oneOrMoreUnselected = false;

    for (let grp of group.groups) {
      switch (this.getSelected(grp)) {
        case Selection.Some:
          return Selection.Some;
        case Selection.None:
          oneOrMoreUnselected = true;
          break;
        case Selection.All:
          oneOrMoreSelected = true;
          break;
      }
    }

    for (let sig of group.signals) {
      if (sig.enabled) oneOrMoreSelected = true;
      else oneOrMoreUnselected = true;
    }

    if (oneOrMoreSelected) {
      if (oneOrMoreUnselected) return Selection.Some;
      else return Selection.All;
    } else {
      return Selection.None;
    }
  }

  updateSelectedSignals() {
    this.selectedSignals.emit(this.signalsToSelect);
  }
}

enum Selection {
  None,
  Some,
  All,
}
